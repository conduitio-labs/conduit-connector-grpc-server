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

	opencdcv1 "github.com/conduitio/conduit-connector-protocol/proto/opencdc/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	var cTypes [1]struct{}
	_ = cTypes[int(sdk.OperationCreate)-int(opencdcv1.Operation_OPERATION_CREATE)]
	_ = cTypes[int(sdk.OperationUpdate)-int(opencdcv1.Operation_OPERATION_UPDATE)]
	_ = cTypes[int(sdk.OperationDelete)-int(opencdcv1.Operation_OPERATION_DELETE)]
	_ = cTypes[int(sdk.OperationSnapshot)-int(opencdcv1.Operation_OPERATION_SNAPSHOT)]
}

func Record(record *opencdcv1.Record) (sdk.Record, error) {
	key, err := Data(record.Key)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error converting key: %w", err)
	}

	payload, err := Change(record.Payload)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("error converting payload: %w", err)
	}

	out := sdk.Record{
		Position:  record.Position,
		Operation: sdk.Operation(record.Operation),
		Metadata:  record.Metadata,
		Key:       key,
		Payload:   payload,
	}
	return out, nil
}

func Change(in *opencdcv1.Change) (sdk.Change, error) {
	before, err := Data(in.Before)
	if err != nil {
		return sdk.Change{}, fmt.Errorf("error converting before: %w", err)
	}

	after, err := Data(in.After)
	if err != nil {
		return sdk.Change{}, fmt.Errorf("error converting after: %w", err)
	}

	out := sdk.Change{
		Before: before,
		After:  after,
	}
	return out, nil
}

func Data(in *opencdcv1.Data) (sdk.Data, error) {
	d := in.GetData()
	if d == nil {
		return nil, nil
	}

	switch v := d.(type) {
	case *opencdcv1.Data_RawData:
		return sdk.RawData(v.RawData), nil
	case *opencdcv1.Data_StructuredData:
		return sdk.StructuredData(v.StructuredData.AsMap()), nil
	default:
		return nil, errors.New("invalid Data type")
	}
}
