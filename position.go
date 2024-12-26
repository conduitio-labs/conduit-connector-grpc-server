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
	"encoding/binary"

	"github.com/conduitio/conduit-commons/opencdc"
)

type Position struct {
	Index    uint32
	Original []byte
}

func ToRecordPosition(p opencdc.Position) Position {
	uint32Value := binary.BigEndian.Uint32(p[:4])
	return Position{
		Index:    uint32Value,
		Original: p[4:],
	}
}

func AttachPositionIndex(p opencdc.Position, index uint32) opencdc.Position {
	indexBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(indexBytes, index)

	//nolint:makezero // intended append
	return append(indexBytes, p...)
}
