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
	"github.com/conduitio/conduit-commons/opencdc"
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

	records := []opencdc.Record{
		{
			Position:  opencdc.Position("foo"),
			Operation: opencdc.OperationCreate,
			Key:       opencdc.StructuredData{"id1": "6"},
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"foo": "bar",
				},
			},
		},
		{
			Position:  opencdc.Position("foobar"),
			Operation: opencdc.OperationSnapshot,
			Key:       opencdc.RawData("bar"),
			Payload: opencdc.Change{
				After: opencdc.RawData("baz"),
			},
		},
		{
			Position:  opencdc.Position("bar"),
			Operation: opencdc.OperationDelete,
			Key:       opencdc.RawData("foobar"),
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"bar": "baz",
				},
			},
		},
	}

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
		got, ok := <-server.RecordCh
		is.True(ok)
		is.Equal(got, rec)
		err = server.SendAck(rec.Position)
		is.NoErr(err)
	}

	// wait for acks to be received
	for i := range records {
		// block until ack is received
		ack, err := stream.Recv()
		is.NoErr(err)
		is.True(bytes.Equal(ack.AckPosition, records[i].Position))
	}
}

func TestServer_StopSignal(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server, err := runServer(t, lis, ctx)
	is.NoErr(err)
	defer server.Close()

	// prepare client
	stream := createTestClient(t, dialer)

	want := opencdc.Record{Position: opencdc.Position("foo")}
	record, err := toproto.Record(want)
	is.NoErr(err)
	err = stream.Send(record)
	is.NoErr(err)

	// read and assert records, send acks
	select {
	case got := <-server.RecordCh:
		is.Equal(got, want)
	case <-time.After(time.Second):
		is.Fail() // record not received in time
	}

	// cancel the open context, which signals a stop
	cancel()

	tomb := server.tomb.Load()
	select {
	case <-(*tomb).Dying():
		// this is expected
	case <-time.After(time.Second):
		is.Fail() // tomb didn't start dying
	}

	err = server.SendAck(want.Position)
	is.NoErr(err)
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
	defer server.Close()

	want := opencdc.Record{
		Position:  opencdc.Position("foo"),
		Operation: opencdc.OperationCreate,
		Key:       opencdc.StructuredData{"id1": "6"},
		Payload: opencdc.Change{
			After: opencdc.StructuredData{
				"foo": "bar",
			},
		},
	}

	// create first client
	stream1 := createTestClient(t, dialer)
	// first client closed the stream with server
	err = stream1.CloseSend()
	is.NoErr(err)

	// second client should be able to connect to server
	stream2 := createTestClient(t, dialer)

	record, err := toproto.Record(want)
	is.NoErr(err)
	// second stream should work
	err = stream2.Send(record)
	is.NoErr(err)

	select {
	case got := <-server.RecordCh:
		is.Equal(got, want)
	case <-time.After(time.Second):
		is.Fail() // record not received in time
	}
}

func runServer(t *testing.T, lis *bufconn.Listener, ctx context.Context) (*Server, error) {
	server := NewServer(ctx)
	grpcSrv := grpc.NewServer()
	pb.RegisterSourceServiceServer(grpcSrv, server)
	go func() {
		if err := grpcSrv.Serve(lis); err != nil {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		grpcSrv.Stop()
	})

	return server, nil
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
	client := pb.NewSourceServiceClient(conn)
	stream, err := client.Stream(ctx)
	is.NoErr(err)
	t.Cleanup(func() {
		err := conn.Close()
		is.NoErr(err)
	})
	return stream
}
