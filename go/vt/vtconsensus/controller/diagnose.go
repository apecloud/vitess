/*
Copyright 2021 The Vitess Authors.
Copyright ApeCloud, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/log"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/vtconsensus/db"
	"vitess.io/vitess/go/vt/vterrors"
)

var pingTabletTimeout = 2 * time.Second

func init() {
	servenv.OnParseFor("vtconsensus", func(fs *pflag.FlagSet) {
		fs.DurationVar(&pingTabletTimeout, "ping_tablet_timeout", 2*time.Second, "time to wait when we ping a tablet")
	})
}

// DiagnoseType is the types of Diagnose result
type DiagnoseType string

const (
	// DiagnoseTypeError represents an DiagnoseTypeError status
	DiagnoseTypeError DiagnoseType = "error"
	// DiagnoseTypeHealthy represents everything is DiagnoseTypeHealthy
	DiagnoseTypeHealthy = "Healthy"
	// DiagnoseTypeUnreachablePrimary represents the primary MySQL is unreachable
	DiagnoseTypeUnreachablePrimary = "UnreachablePrimary"
	// DiagnoseTypeUnreachableReplica represents the replica MySQL is unreachable
	DiagnoseTypeUnreachableReplica = "UnreachableReplica"
	// DiagnoseTypeUnreachableLeader represents the primary MySQL is unreachable
	DiagnoseTypeUnreachableLeader = "UnreachableLeader"
	// DiagnoseTypeUnreachableFollower represents the replica MySQL is unreachable
	DiagnoseTypeUnreachableFollower = "UnreachableFollower"
	// DiagnoseTypeWrongPrimaryTablet represents the primary tablet is incorrect based on on apecloud mysql
	DiagnoseTypeWrongPrimaryTablet = "WrongPrimaryTablet"
	// DiagnoseTypeMissingConsensusLeader represents miss consensus leader on apecloud mysql
	DiagnoseTypeMissingConsensusLeader = "MissingConsensusLeader"
	// DiagnoseTypeUnconnectedReplica represents cluster with primary tablet, but a node is not connected to it
	DiagnoseTypeUnconnectedReplica = "UnconnectedReplica"
	// diagnoseTypeUnknown represents a unclear intermediate diagnose state
	diagnoseTypeUnknown = "Unknown"
)

// ScanAndRepairShard scans a particular shard by first Diagnose the shard with info from consensusShard
// and then repair the problem if the shard is unhealthy
func (shard *ConsensusShard) ScanAndRepairShard(ctx context.Context) {
	shard.logger.Infof("ScanAndRepairShard diagnose %v status", formatKeyspaceShard(shard.KeyspaceShard))
	status, err := shard.Diagnose(ctx)
	if err != nil {
		shard.logger.Infof("fail to scanAndRepairShard %v/%v because of diagnose error: %v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard, err)
	}
	// We are able to get Diagnose without error.
	// Note: all the recovery function should first try to grab a shard level lock
	// and check the trigger conditions before doing anything. This is to avoid
	// other VTConsensus instance try to do the same thing
	shard.logger.Infof("%v status is %v", formatKeyspaceShard(shard.KeyspaceShard), status)
	if _, err := shard.Repair(ctx, status); err != nil {
		shard.logger.Errorf("failed to ScanAndRepairShard repair %v: %v", status, err)
	}
}

// Diagnose the shard in the following order:
// 1. if the shard has a group that every node agreed on
// 2. if the group has any active (online / recovering) member
// 3. if the shard has initialized a Vitess primary
// 4. if primary tablet is reachable
// 5. if Vitess primary and mysql primary reconciled
// 6. if we have enough group members
// 7. if the primary node has read_only=OFF
// 8. if there is a node that is not in Mysql group
func (shard *ConsensusShard) Diagnose(ctx context.Context) (DiagnoseType, error) {
	shard.Lock()
	defer shard.Unlock()
	diagnoseResult, err := shard.diagnoseLocked(ctx)
	shard.shardStatusCollector.recordDiagnoseResult(diagnoseResult)
	shard.populateVTConsensusStatusLocked()
	if diagnoseResult != DiagnoseTypeHealthy {
		shard.logger.Warningf(`VTConsensus diagnose shard as unhealthy for %s/%s:\n result=%v, last_result=%v, instances=%v, primary=%v, primary_tablet=%v, problematics=%v, unreachables=%v,\n%v`,
			shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard,
			shard.shardStatusCollector.status.DiagnoseResult,
			shard.lastDiagnoseResult,
			shard.shardStatusCollector.status.Instances,
			shard.shardStatusCollector.status.Primary,
			shard.primaryTabletAlias(),
			shard.shardStatusCollector.status.Problematics,
			shard.shardStatusCollector.status.Unreachables,
			shard.sqlConsensusView.ToString())
	}
	if diagnoseResult != shard.lastDiagnoseResult {
		shard.lastDiagnoseResult = diagnoseResult
		shard.lastDiagnoseSince = time.Now()
	}
	return diagnoseResult, err
}

func (shard *ConsensusShard) diagnoseLocked(ctx context.Context) (DiagnoseType, error) {
	// First, check primary healthy.
	// force refresh consensus view
	// TODO add fast path: directly fetch leader local view by primary tablet, if primary tablet exist.
	err := shard.refreshSQLConsensusView()
	if err != nil {
		return DiagnoseTypeError, vterrors.Wrap(err, "fail to refreshConsensusView")
	}

	if shard.isActive.Get() {
		diagnoseType, err := shard.checkPrimaryTablet(ctx)

		if err != nil {
			shard.logger.Infof("Diagnose %v", err)
		}
		switch diagnoseType {
		case DiagnoseTypeHealthy:
			return DiagnoseTypeHealthy, nil
		case DiagnoseTypeMissingConsensusLeader:
			// we need fetch consensus info from all the nodes
			err := shard.refreshSQLConsensusView()
			if err != nil {
				return DiagnoseTypeError, vterrors.Wrap(err, "fail to refreshConsensusView")
			}
		default:
			return diagnoseType, nil
		}
	}
	if shard.isActive.Get() {
		diagnoseType, err := shard.checkPrimaryTablet(ctx)
		return diagnoseType, err
	}
	// If we get here, shard is DiagnoseTypeHealthy
	return DiagnoseTypeHealthy, nil
}

func (shard *ConsensusShard) fastPathDiagnose(ctx context.Context) DiagnoseType {
	pHost, pPort, isOnline := shard.sqlConsensusView.GetPrimary()
	primaryTablet := shard.findShardPrimaryTablet()

	// if no primary tablet
	if primaryTablet == nil {
		log.Infof("Primary tablet unknown %v/%v", shard.KeyspaceShard.Keyspace, shard.KeyspaceShard.Shard)
		return DiagnoseTypeWrongPrimaryTablet
	}

	// if mysql don't maybe elect leader, or ConsensusGlobaView is null when vtconsensus startup.
	if !isOnline || pHost == "" || pPort == 0 {
		return DiagnoseTypeWrongPrimaryTablet
	}
	// if mysql leader mismatch tablet type , maybe mysql failover leads to switch leader.
	// so need change tablet type.
	if primaryTablet.instanceKey.Hostname != pHost || primaryTablet.instanceKey.Port != pPort {
		// we find a mismatch but if the reported mysql primary is not in
		// topology we should consider it as unreachable.
		if shard.findTabletByHostAndPort(pHost, pPort) == nil {
			return DiagnoseTypeUnreachablePrimary
		}
		return DiagnoseTypeWrongPrimaryTablet
	}
	return DiagnoseTypeHealthy
}

func (shard *ConsensusShard) checkPrimaryTablet(ctx context.Context) (DiagnoseType, error) {
	// Find out the hostname and port of the Leader in consensus global view.
	host, port, isOnline := shard.sqlConsensusView.GetPrimary()
	// if we failed to find leader for apecloud mysql, maybe consensus cluster no ready.
	// return true , vtconsensus need force refreshSQLConsensusView.
	if !isOnline || host == "" || port == 0 {
		shard.logger.Warningf("apecloud mysql consensus no Leader %v:%v", host, port)
		return DiagnoseTypeMissingConsensusLeader, errMissingConsensusLeader
	}

	// Make sure we have a primary tablet available
	// findTabletByHostAndPort returns nil when we cannot find a tablet
	// that is running on host:port, which means the tablet get stuck
	// or when the tablet is not reachable
	// we return errMissingPrimaryTablet, we need to repair tablet manually.
	tablet := shard.findTabletByHostAndPort(host, port)
	if tablet == nil || !shard.instanceReachable(ctx, tablet) {
		shard.logger.Errorf("Failed to find tablet that is running with mysql on %v:%v", host, port)
		return DiagnoseTypeUnreachablePrimary, errUnreachablePrimaryTablet
	}

	// find primary tablet in the cluster
	primary := shard.findShardPrimaryTablet()
	// If we failed to find primary for shard, it mostly means we are initializing the shard
	// return true directly so that vtconsensus will set primary tablet according to MySQL group
	if primary == nil || (host != primary.instanceKey.Hostname) || (port != primary.instanceKey.Port) {
		shard.logger.Infof("unable to find primary tablet for %v", formatKeyspaceShard(shard.KeyspaceShard))
		return DiagnoseTypeWrongPrimaryTablet, errWrongPrimaryTablet
	}

	return DiagnoseTypeHealthy, nil
}

func (shard *ConsensusShard) isPrimaryReachable(ctx context.Context) (bool, error) {
	primaryTablet := shard.findShardPrimaryTablet()
	if primaryTablet == nil {
		return false, fmt.Errorf("unable to find primary for %v", formatKeyspaceShard(shard.KeyspaceShard))
	}
	return shard.instanceReachable(ctx, primaryTablet), nil
}

func (shard *ConsensusShard) instanceReachable(ctx context.Context, instance *consensusInstance) bool {
	pingCtx, cancel := context.WithTimeout(context.Background(), pingTabletTimeout)
	defer cancel()
	c := make(chan error, 1)
	// tmc.Ping create grpc client connection first without timeout via dial
	// then call the grpc endpoint using the context with timeout
	// this is problematic if the host is really unreachable, we have to wait the
	// all the retries inside grpc.dial with exponential backoff
	go func() { c <- shard.tmc.Ping(pingCtx, instance.tablet) }()
	select {
	case <-pingCtx.Done():
		shard.logger.Errorf("Ping abort timeout %v", pingTabletTimeout)
		return false
	case err := <-c:
		if err != nil {
			shard.logger.Errorf("Ping error host=%v: %v", instance.instanceKey.Hostname, err)
		}
		return err == nil
	}
}

// findShardPrimaryTablet returns the primary for the shard
// it is either based on shard info from global topo or based on tablet types
// from local topo
func (shard *ConsensusShard) findShardPrimaryTablet() *consensusInstance {
	var primaryInstance *consensusInstance
	for _, instance := range shard.instances {
		if shard.PrimaryAlias == instance.alias {
			return instance
		}
	}
	return primaryInstance
}

func (shard *ConsensusShard) primaryTabletAlias() string {
	primary := shard.findShardPrimaryTablet()
	if primary == nil {
		return "UNKNOWN"
	}
	return primary.alias
}

func (collector *shardStatusCollector) recordDiagnoseResult(result DiagnoseType) {
	collector.Lock()
	defer collector.Unlock()
	collector.status.DiagnoseResult = result
}

func (collector *shardStatusCollector) recordUnreachables(instance *consensusInstance) {
	collector.Lock()
	defer collector.Unlock()
	// dedup
	// the list size is at most same as number instances in a shard so iterate to dedup is not terrible
	for _, alias := range collector.status.Unreachables {
		if alias == instance.alias {
			return
		}
	}
	collector.status.Unreachables = append(collector.status.Unreachables, instance.alias)
}

func (collector *shardStatusCollector) clear() {
	collector.Lock()
	defer collector.Unlock()
	collector.status.Unreachables = nil
	collector.status.Problematics = nil
}

func (collector *shardStatusCollector) recordProblematics(instance *consensusInstance) {
	collector.Lock()
	defer collector.Unlock()
	// dedup
	// the list size is at most same as number instances in a shard so iterate to dedup is not terrible
	for _, alias := range collector.status.Problematics {
		if alias == instance.alias {
			return
		}
	}
	collector.status.Problematics = append(collector.status.Problematics, instance.alias)
}

func formatKeyspaceShard(keyspaceShard *topo.KeyspaceShard) string {
	return fmt.Sprintf("%v/%v", keyspaceShard.Keyspace, keyspaceShard.Shard)
}

func isHostPortValid(host string, port int) bool {
	return host != "" && port != 0
}

func unreachableError(err error) bool {
	contains := []string{
		// "no such host"/"no route to host" is the error when a host is not reachalbe
		"no such host",
		"no route to host",
		// "connect: connection refused" is the error when a mysqld refused the connection
		"connect: connection refused",
		// "invalid mysql instance key" is the error when a tablet does not populate mysql hostname or port
		// this can happen if the tablet crashed. We keep them in the grShard.instances list to compute
		// quorum but consider it as an unreachable host.
		"invalid mysql instance key",
	}
	for _, k := range contains {
		if strings.Contains(err.Error(), k) {
			return true
		}
	}
	return false
}

// refreshSQLConsensusView hits all instances and renders a SQL group locally for later diagnoses
// the SQL group contains a list of "views" for the group from all the available nodes
func (shard *ConsensusShard) refreshSQLConsensusView() error {
	var view *db.ConsensusGlobalView
	var localCount int
	var leaderHost string
	var leaderPort int
	var leaderServerId int
	var leaderTerm int
	var leaderInstance *consensusInstance
	view = shard.dbAgent.NewConsensusGlobalView()

	// reset views in sql group
	shard.sqlConsensusView.clear()
	for _, instance := range shard.instances {
		var err error
		var localView *db.ConsensusLocalView
		localView, err = shard.dbAgent.FetchConsensusLocalView(instance.alias, instance.instanceKey, view)
		if err != nil {
			shard.shardStatusCollector.recordProblematics(instance)
			if unreachableError(err) {
				shard.shardStatusCollector.recordUnreachables(instance)
			}
			shard.logger.Errorf("%v error while fetch local view from apecloud mysql: %v", instance.alias, err)
			// Only raise error if we failed to get any data from mysql
			// maybe some mysql node is not start.
			continue
		}

		// if exists dual leader, greater term instance would be new leader.
		if localView.Role == db.LEADER &&
			(nil == leaderInstance || leaderTerm < localView.CurrentTerm) {
			leaderInstance = instance
			leaderHost = instance.instanceKey.Hostname
			leaderPort = instance.instanceKey.Port
			leaderServerId = localView.ServerID
			leaderTerm = localView.CurrentTerm
		}
		localCount++
	}

	if localCount > 0 && leaderInstance != nil {
		var err error
		shard.logger.Infof("get consensus leader serverid %d: %v:%v", leaderServerId, leaderHost, leaderPort)

		// need use mysql expose host:port, not param localhost:@@port
		view.LeaderMySQLHost = leaderHost
		view.LeaderMySQLPort = leaderPort
		view.LeaderServerID = leaderServerId
		err = shard.dbAgent.FetchConsensusGlobalView(view)
		if err != nil {
			shard.logger.Errorf("%v:%v error while fetch global view from apecloud mysql: %v",
				leaderHost, leaderPort, err)
			shard.sqlConsensusView.recordView(nil)
			return errUnreachableLeaderMySQL
		}
	}

	shard.sqlConsensusView.recordView(view)
	return nil
}

func (shard *ConsensusShard) disconnectedInstance() (*consensusInstance, error) {
	primaryInstance := shard.findShardPrimaryTablet()
	// if there is no primary, we should recover from DiagnoseTypeWrongPrimaryTablet
	if primaryInstance == nil {
		return nil, fmt.Errorf("%v does not have primary", formatKeyspaceShard(shard.KeyspaceShard))
	}
	rand.Shuffle(len(shard.instances), func(i, j int) {
		shard.instances[i], shard.instances[j] = shard.instances[j], shard.instances[i]
	})
	for _, instance := range shard.instances {
		isUnconnected := shard.sqlConsensusView.IsUnconnectedReplica(instance.instanceKey)
		if isUnconnected {
			return instance, fmt.Errorf("%v some node unconnected", formatKeyspaceShard(shard.KeyspaceShard))
		}
	}
	return nil, nil
}
