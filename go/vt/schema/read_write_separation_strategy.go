/*
Copyright 2021 The Vitess Authors.

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

package schema

import (
	"fmt"
	"regexp"
)

var (
	readWriteSeparationStrategyParserRegexp = regexp.MustCompile(`^([\S]+)\s+(.*)$`)
)

type ReadWriteSeparationStrategy string

const (
	// ReadWriteSeparationStrategyEnable enables read write separation
	ReadWriteSeparationStrategyEnable ReadWriteSeparationStrategy = "enable"
	// ReadWriteSeparationStrategyDisable disables read write separation
	ReadWriteSeparationStrategyDisable ReadWriteSeparationStrategy = "disable"
)

// IsEnabled returns true if the strategy is enabled
func (s ReadWriteSeparationStrategy) IsEnabled() bool {
	return s == ReadWriteSeparationStrategyEnable
}

type ReadWriteSeparationStrategySetting struct {
	Strategy ReadWriteSeparationStrategy `json:"strategy,omitempty"`
	Options  string                      `json:"options,omitempty"`
}

func NewReadWriteSeparationStrategySettingSetting(strategy ReadWriteSeparationStrategy, options string) *ReadWriteSeparationStrategySetting {
	return &ReadWriteSeparationStrategySetting{
		Strategy: strategy,
		Options:  options,
	}
}

func ParseReadWriteSeparationStrategy(strategyVariable string) (*ReadWriteSeparationStrategySetting, error) {
	setting := &ReadWriteSeparationStrategySetting{}
	strategyName := strategyVariable
	if submatch := readWriteSeparationStrategyParserRegexp.FindStringSubmatch(strategyVariable); len(submatch) > 0 {
		strategyName = submatch[1]
		setting.Options = submatch[2]
	}

	switch strategy := ReadWriteSeparationStrategy(strategyName); strategy {
	case "": // backward compatiblity and to handle unspecified values
		setting.Strategy = ReadWriteSeparationStrategyDisable
	case ReadWriteSeparationStrategyEnable, ReadWriteSeparationStrategyDisable:
		setting.Strategy = strategy
	default:
		return nil, fmt.Errorf("Unknown ReadWriteSeparationStrategy: '%v'", strategy)
	}
	return setting, nil
}

// ToString returns a simple string representation of this instance
func (setting *ReadWriteSeparationStrategySetting) ToString() string {
	return fmt.Sprintf("ReadWriteSeparationStrategySetting: strategy=%v, options=%s", setting.Strategy, setting.Options)
}
