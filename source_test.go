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

package grpcserver

import (
	"bytes"
	"context"
	"errors"
	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	"github.com/conduitio-labs/conduit-connector-grpc-server/toproto"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"net"
	"testing"
	"time"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestRead_Success(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	records := []sdk.Record{
		{
			Position:  sdk.Position("foo"),
			Operation: sdk.OperationCreate,
			Key:       sdk.StructuredData{"id1": "6"},
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"foo": "bar",
				},
			},
		},
		{
			Position:  sdk.Position("foobar"),
			Operation: sdk.OperationSnapshot,
			Key:       sdk.RawData("bar"),
			Payload: sdk.Change{
				After: sdk.RawData("baz"),
			},
		},
		{
			Position:  sdk.Position("bar"),
			Operation: sdk.OperationDelete,
			Key:       sdk.RawData("foobar"),
			Payload: sdk.Change{
				After: sdk.StructuredData{
					"bar": "baz",
				},
			},
		},
	}

	ctx := context.Background()
	src := NewSourceWithListener(lis)
	err := src.Configure(ctx, map[string]string{"url": "bufnet"})
	is.NoErr(err)
	err = src.Open(ctx, nil)
	is.NoErr(err)
	defer func(src sdk.Source, ctx context.Context) {
		err := src.Teardown(ctx)
		is.NoErr(err)
	}(src, ctx)

	// prepare client
	stream := createTestClient(t, dialer)
	go func() {
		for _, r := range records {
			record, err := toproto.Record(r)
			is.NoErr(err)
			err = stream.Send(record)
			is.NoErr(err)
		}
	}()

	// read and assert records, send acks
	for _, rec := range records {
		got, err := src.Read(ctx)
		is.NoErr(err)
		is.Equal(got, rec)
		err = src.Ack(ctx, rec.Position)
		is.NoErr(err)
	}
	// wait for ack to be received
	for i := range records {
		// block until ack is received
		ack, err := stream.Recv()
		is.NoErr(err)
		is.True(bytes.Equal(ack.AckPosition, records[i].Position))
	}
}

func TestRead_CloseListener(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)

	ctx := context.Background()
	src := NewSourceWithListener(lis)
	err := src.Configure(ctx, map[string]string{"url": "bufnet"})
	is.NoErr(err)
	err = src.Open(ctx, nil)
	is.NoErr(err)
	defer func(src sdk.Source, ctx context.Context) {
		err := src.Teardown(ctx)
		is.NoErr(err)
	}(src, ctx)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond*100)
	defer cancel()
	_, err = src.Read(timeoutCtx)
	is.True(errors.Is(err, context.DeadlineExceeded))

	err = lis.Close()
	is.NoErr(err)

	_, err = src.Read(ctx)
	is.True(err != nil)
	is.True(!errors.Is(err, context.DeadlineExceeded))
}

func createTestClient(t *testing.T, dialer func(ctx context.Context, _ string) (net.Conn, error)) pb.SourceService_StreamClient {
	is := is.New(t)
	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		"bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(dialer),
	)
	is.NoErr(err)
	t.Cleanup(func() {
		err = conn.Close()
		is.NoErr(err)
	})
	client := pb.NewSourceServiceClient(conn)
	stream, err := client.Stream(ctx)
	is.NoErr(err)
	return stream
}
