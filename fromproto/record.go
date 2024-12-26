// Copyright © 2023 Meroxa, Inc.
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

package fromproto

import (
	"errors"
	"fmt"

	"github.com/conduitio/conduit-commons/opencdc"
	opencdcv1 "github.com/conduitio/conduit-commons/proto/opencdc/v1"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(opencdc.OperationCreate)-int(opencdcv1.Operation_OPERATION_CREATE)]
	_ = cTypes[int(opencdc.OperationUpdate)-int(opencdcv1.Operation_OPERATION_UPDATE)]
	_ = cTypes[int(opencdc.OperationDelete)-int(opencdcv1.Operation_OPERATION_DELETE)]
	_ = cTypes[int(opencdc.OperationSnapshot)-int(opencdcv1.Operation_OPERATION_SNAPSHOT)]
}

func Record(record *opencdcv1.Record) (opencdc.Record, error) {
	key, err := Data(record.Key)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error converting key: %w", err)
	}

	payload, err := Change(record.Payload)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("error converting payload: %w", err)
	}

	out := opencdc.Record{
		Position:  record.Position,
		Operation: opencdc.Operation(record.Operation),
		Metadata:  record.Metadata,
		Key:       key,
		Payload:   payload,
	}
	return out, nil
}

func Change(in *opencdcv1.Change) (opencdc.Change, error) {
	before, err := Data(in.Before)
	if err != nil {
		return opencdc.Change{}, fmt.Errorf("error converting before: %w", err)
	}

	after, err := Data(in.After)
	if err != nil {
		return opencdc.Change{}, fmt.Errorf("error converting after: %w", err)
	}

	out := opencdc.Change{
		Before: before,
		After:  after,
	}
	return out, nil
}

func Data(in *opencdcv1.Data) (opencdc.Data, error) {
	d := in.GetData()
	if d == nil {
		return nil, nil
	}

	switch v := d.(type) {
	case *opencdcv1.Data_RawData:
		return opencdc.RawData(v.RawData), nil
	case *opencdcv1.Data_StructuredData:
		return opencdc.StructuredData(v.StructuredData.AsMap()), nil
	default:
		return nil, errors.New("invalid Data type")
	}
}
