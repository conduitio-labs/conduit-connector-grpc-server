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

package source

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	"github.com/conduitio-labs/conduit-connector-grpc-server/toproto"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestServer_Success(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	ctx := context.Background()
	server, err := runServer(t, lis, ctx)
	is.NoErr(err)
	defer server.Close()

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

	// prepare client
	stream := createTestClient(t, "bufnet1", dialer)
	go func() {
		err := sendExpectedRecords(t, stream, records)
		is.NoErr(err)
	}()

	// read and assert records, send acks
	for _, rec := range records {
		got, ok := <-server.RecordCh
		is.True(ok)
		is.Equal(got, rec)
		err = server.SendAck(rec.Position)
		is.NoErr(err)
	}
	// wait for acks to be received
	time.Sleep(500 * time.Millisecond)
}

func TestServer_ClientStreamClosed(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	ctx := context.Background()
	server, err := runServer(t, lis, ctx)
	is.NoErr(err)
	t.Cleanup(func() {
		server.Close()
	})

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
	}

	// create first client
	stream1 := createTestClient(t, "bufnet4", dialer)
	// first client closed the stream with server
	err = stream1.CloseSend()
	is.NoErr(err)
	time.Sleep(1 * time.Second)

	// second client should be able to connect to server
	stream2 := createTestClient(t, "bufnet5", dialer)

	record, err := toproto.Record(records[0])
	is.NoErr(err)
	// second stream should work
	err = stream2.Send(record)
	is.NoErr(err)
}

func runServer(t *testing.T, lis *bufconn.Listener, ctx context.Context) (*Server, error) {
	server := NewServer(ctx)
	grpcSrv := grpc.NewServer()
	pb.RegisterSourceServiceServer(grpcSrv, server)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			return
		}
	}()
	t.Cleanup(func() {
		grpcSrv.Stop()
	})

	return server, nil
}

func createTestClient(t *testing.T, target string, dialer func(ctx context.Context, _ string) (net.Conn, error)) pb.SourceService_StreamClient {
	is := is.New(t)
	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithContextDialer(dialer),
	)
	is.NoErr(err)
	client := pb.NewSourceServiceClient(conn)
	stream, err := client.Stream(ctx)
	is.NoErr(err)
	t.Cleanup(func() {
		err := conn.Close()
		is.NoErr(err)
	})
	return stream
}

func sendExpectedRecords(t *testing.T, stream pb.SourceService_StreamClient, records []sdk.Record) error {
	is := is.New(t)
	for _, r := range records {
		record, err := toproto.Record(r)
		is.NoErr(err)
		err = stream.Send(record)
		is.NoErr(err)
	}
	for i := range records {
		// block until ack is received
		ack, err := stream.Recv()
		is.NoErr(err)
		is.True(bytes.Equal(ack.AckPosition, records[i].Position))
	}
	return nil
}
