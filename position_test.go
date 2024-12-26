// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package grpcserver

import (
	"bytes"
	"testing"

	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
)

func TestPosition_ToSDKPosition(t *testing.T) {
	positionTests := []struct {
		name    string
		wantErr bool
		in      Position
		out     opencdc.Position
	}{
		{
			name:    "zero position",
			wantErr: false,
			in: Position{
				Index:    uint32(0),
				Original: []byte("01"),
			},
			out: []byte{0, 0, 0, 0, 48, 49},
		},
		{
			name:    "non-zero position",
			wantErr: false,
			in: Position{
				Index:    uint32(11),
				Original: []byte("11"),
			},
			out: []byte{0, 0, 0, 11, 49, 49},
		},
		{
			name:    "high position",
			wantErr: false,
			in: Position{
				Index:    uint32(999999999),
				Original: []byte("12"),
			},
			out: []byte{59, 154, 201, 255, 49, 50},
		},
	}

	for _, tt := range positionTests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			p := AttachPositionIndex(tt.in.Original, tt.in.Index)
			is.True(bytes.Equal(p, tt.out))
			pos := ToRecordPosition(p)
			is.Equal(pos, tt.in)
		})
	}
}
