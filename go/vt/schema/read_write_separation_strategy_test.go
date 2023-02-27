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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsEnabled(t *testing.T) {
	assert.True(t, ReadWriteSeparationStrategyEnable.IsEnabled())
	assert.False(t, ReadWriteSeparationStrategyDisable.IsEnabled())
	assert.False(t, ReadWriteSeparationStrategy("").IsEnabled())
	assert.True(t, ReadWriteSeparationStrategy("enable").IsEnabled())
	assert.False(t, ReadWriteSeparationStrategy("disable").IsEnabled())
}

func TestParseReadWriteSeparationStrategy(t *testing.T) {
	type args struct {
		strategyVariable string
	}
	tests := []struct {
		name    string
		args    args
		want    *ReadWriteSeparationStrategySetting
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "enable",
			args: args{
				strategyVariable: "enable",
			},
			want: &ReadWriteSeparationStrategySetting{
				Strategy: ReadWriteSeparationStrategyEnable,
			},
			wantErr: assert.NoError,
		},
		{
			name: "disable",
			args: args{
				strategyVariable: "disable",
			},
			want: &ReadWriteSeparationStrategySetting{
				Strategy: ReadWriteSeparationStrategyDisable,
			},
			wantErr: assert.NoError,
		},
		{
			name: "",
			args: args{
				strategyVariable: "disable",
			},
			want: &ReadWriteSeparationStrategySetting{
				Strategy: ReadWriteSeparationStrategyDisable,
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseReadWriteSeparationStrategy(tt.args.strategyVariable)
			if !tt.wantErr(t, err, fmt.Sprintf("ParseReadWriteSeparationStrategy(%v)", tt.args.strategyVariable)) {
				return
			}
			assert.Equalf(t, tt.want, got, "ParseReadWriteSeparationStrategy(%v)", tt.args.strategyVariable)
		})
	}
}
