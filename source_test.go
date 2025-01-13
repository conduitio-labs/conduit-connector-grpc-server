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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	"github.com/conduitio-labs/conduit-connector-grpc-server/toproto"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const (
	clientCertPath = "./test/certs/client.crt"
	clientKeyPath  = "./test/certs/client.key"
	serverCertPath = "./test/certs/server.crt"
	serverKeyPath  = "./test/certs/server.key"
	caCertPath     = "./test/certs/ca.crt"
)

func TestTeardownSource_NoOpen(t *testing.T) {
	is := is.New(t)
	con := NewSource()
	err := con.Teardown(context.Background())
	is.NoErr(err)
}

func TestConfigure_DisableMTLS(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	src := NewSource()
	err := src.Configure(ctx, map[string]string{
		"url":                  "localhost",
		"mtls.disabled":        "false",
		"mtls.server.certPath": "", // empty path, should fail
		"mtls.server.keyPath":  serverKeyPath,
		"mtls.ca.certPath":     caCertPath,
	})
	is.True(err != nil)
	err = src.Configure(ctx, map[string]string{
		"url":                  "localhost",
		"mtls.disabled":        "true", // disabled
		"mtls.server.certPath": "",     // should be ok
	})
	is.NoErr(err)
}

func TestRead_Success(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}

	records := []opencdc.Record{
		{
			Position:  opencdc.Position("foo"),
			Operation: opencdc.OperationCreate,
			Metadata:  opencdc.Metadata{},
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
			Metadata:  opencdc.Metadata{},
			Key:       opencdc.RawData("foobar"),
			Payload: opencdc.Change{
				After: opencdc.StructuredData{
					"bar": "baz",
				},
			},
		},
	}

	ctx := context.Background()
	src := NewSourceWithListener(lis)
	err := src.Configure(ctx, map[string]string{
		"url":                  "bufnet",
		"mtls.server.certPath": serverCertPath,
		"mtls.server.keyPath":  serverKeyPath,
		"mtls.ca.certPath":     caCertPath,
	})
	is.NoErr(err)
	err = src.Open(ctx, nil)
	is.NoErr(err)
	defer func(src sdk.Source, ctx context.Context) {
		err := src.Teardown(ctx)
		is.NoErr(err)
	}(src, ctx)

	// prepare client
	stream := createTestClient(t, true, dialer)
	go func() {
		for i, r := range records {
			r.Position = AttachPositionIndex(r.Position, uint32(i)) //nolint:gosec //ignore.
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
	// wait for ack to be received on the client side
	for i := range records {
		// block until ack is received
		ack, err := stream.Recv()
		is.NoErr(err)
		// client expects the first 4 bytes of the position to be the index.
		// detach the index to compare positions
		is.True(bytes.Equal(ack.AckPosition[4:], records[i].Position))
	}
}

func TestRead_CloseListener(t *testing.T) {
	is := is.New(t)
	// use in-memory connection
	lis := bufconn.Listen(1024 * 1024)

	ctx := context.Background()
	src := NewSourceWithListener(lis)
	err := src.Configure(ctx, map[string]string{
		"url":           "localhost",
		"mtls.disabled": "true",
	})
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

func createTestClient(t *testing.T, enableMTLS bool, dialer func(ctx context.Context, _ string) (net.Conn, error)) pb.SourceService_StreamClient {
	is := is.New(t)
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(dialer),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
		grpc.WithBlock(),
	}
	if enableMTLS {
		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		is.NoErr(err)
		caCert, err := os.ReadFile(caCertPath)
		is.NoErr(err)
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		// create TLS credentials with mTLS configuration
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caCertPool,
			MinVersion:   tls.VersionTLS13,
		})
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(creds))
	} else {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	ctx := context.Background()
	conn, err := grpc.NewClient("localhost", dialOptions...)
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
