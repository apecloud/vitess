package vtgate

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/proto/topodata"
)

func Test_suggestTabletType_to_replica(t *testing.T) {
	type args struct {
		readWriteSeparationStrategy string
		inTransaction               bool
		hasCreatedTempTables        bool
		hasAdvisoryLock             bool
		sql                         string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "select * from users union all select * from users;",
			},
			wantTabletType: topodata.TabletType_REPLICA,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_to_primary(t *testing.T) {
	type args struct {
		readWriteSeparationStrategy string
		inTransaction               bool
		hasCreatedTempTables        bool
		hasAdvisoryLock             bool
		sql                         string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSeparationStrategy=disable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "disable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=disable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "disable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "select * from users union all select * from users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "INSERT INTO users (id, name) VALUES (1, 'foo');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "UPDATE users SET name = 'foo' WHERE id = 1;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "DELETE FROM users WHERE id = 1;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}

func Test_suggestTabletType_force_primary(t *testing.T) {
	type args struct {
		readWriteSeparationStrategy string
		inTransaction               bool
		hasCreatedTempTables        bool
		hasAdvisoryLock             bool
		sql                         string
	}
	tests := []struct {
		name           string
		args           args
		wantTabletType topodata.TabletType
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=true, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               true,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=true, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        true,
				hasAdvisoryLock:             false,
				sql:                         "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=true",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             true,
				sql:                         "SELECT * FROM users;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT last_insert_id();",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT * FROM users lock in share mode;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT * FROM users for update;",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT get_lock('lock', 10);",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT release_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT is_used_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT is_free_lock('lock');",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
		{
			name: "readWriteSeparationStrategy=enable, inTransaction=false, hasCreatedTempTables=false, hasAdvisoryLock=false",
			args: args{
				readWriteSeparationStrategy: "enable",
				inTransaction:               false,
				hasCreatedTempTables:        false,
				hasAdvisoryLock:             false,
				sql:                         "SELECT release_all_locks();",
			},
			wantTabletType: topodata.TabletType_PRIMARY,
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTabletType, err := suggestTabletType(tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
			if !tt.wantErr(t, err, fmt.Sprintf("suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)) {
				return
			}
			assert.Equalf(t, tt.wantTabletType, gotTabletType, "suggestTabletType(%v, %v, %v, %v, %v)", tt.args.readWriteSeparationStrategy, tt.args.inTransaction, tt.args.hasCreatedTempTables, tt.args.hasAdvisoryLock, tt.args.sql)
		})
	}
}
