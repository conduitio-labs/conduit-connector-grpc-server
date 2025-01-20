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

//go:generate paramgen -output=paramgen_src.go SourceConfig

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	"github.com/conduitio-labs/conduit-connector-grpc-server/source"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Source struct {
	sdk.UnimplementedSource

	config SourceConfig
	server *source.Server

	// for stopping the server
	grpcSrv    *grpc.Server
	errCh      chan error
	indexQueue *Queue

	// used only for injecting a listener in tests
	listener net.Listener

	// mTLS
	serverCert tls.Certificate
	caCertPool *x509.CertPool
}

type SourceConfig struct {
	Config
}

// NewSourceWithListener for testing purposes.
func NewSourceWithListener(lis net.Listener) sdk.Source {
	return sdk.SourceWithMiddleware(&Source{listener: lis}, sdk.DefaultSourceMiddleware()...)
}

func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

func (s *Source) Parameters() config.Parameters {
	return s.config.Parameters()
}

func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	sdk.Logger(ctx).Info().Msg("Configuring Source...")
	err := sdk.Util.ParseConfig(ctx, cfg, &s.config, NewSource().Parameters())
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	if !s.config.MTLS.Disabled {
		s.serverCert, s.caCertPool, err = s.config.MTLS.ParseMTLSFiles()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Source) Open(ctx context.Context, _ opencdc.Position) error {
	sdk.Logger(ctx).Info().Msg("Opening Source...")
	s.server = source.NewServer(ctx)
	err := s.runServer()
	if err != nil {
		return err
	}
	s.indexQueue = &Queue{}
	return nil
}

func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	select {
	case <-ctx.Done():
		return opencdc.Record{}, ctx.Err()
	case record, ok := <-s.server.RecordCh:
		if !ok {
			return opencdc.Record{}, fmt.Errorf("record channel is closed")
		}
		pos := ToRecordPosition(record.Position)
		// send the original position without the index to the destination
		record.Position = pos.Original
		// add the index to a queue, so we could attach it again when sending the ack
		s.indexQueue.Enqueue(pos.Index)
		return record, nil
	case err := <-s.errCh:
		return opencdc.Record{}, fmt.Errorf("gRPC server error: %w", err)
	}
}

func (s *Source) Ack(ctx context.Context, position opencdc.Position) error {
	sdk.Logger(ctx).Debug().Str("position", string(position)).Msg("got ack")
	index, err := s.indexQueue.Dequeue()
	if err != nil {
		return fmt.Errorf("unexpected behaviour, an ack is not expected to be received, all records sent to conduit were already acked, %w", err)
	}
	position = AttachPositionIndex(position, index)
	return s.server.SendAck(position)
}

func (s *Source) Teardown(ctx context.Context) error {
	if s.server != nil {
		s.server.Close()
	}
	if s.grpcSrv != nil {
		s.grpcSrv.Stop()
		// check if an error happened while stopping server
		err, ok := <-s.errCh
		if err != nil {
			return err
		}
		if ok {
			close(s.errCh)
		}
	}
	return nil
}

func (s *Source) runServer() error {
	// listener can be set for test purposes
	if s.listener == nil {
		lis, err := net.Listen("tcp", s.config.URL)
		if err != nil {
			return fmt.Errorf("failed to listen: %w", err)
		}
		s.listener = lis
	}

	serverOptions := make([]grpc.ServerOption, 0, 1)
	if !s.config.MTLS.Disabled {
		// create TLS credentials with mTLS configuration
		creds := credentials.NewTLS(&tls.Config{
			Certificates: []tls.Certificate{s.serverCert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    s.caCertPool,
			MinVersion:   tls.VersionTLS13,
		})
		serverOptions = append(serverOptions, grpc.Creds(creds))
	}
	s.grpcSrv = grpc.NewServer(serverOptions...)
	pb.RegisterSourceServiceServer(s.grpcSrv, s.server)

	s.errCh = make(chan error)
	go func() {
		if err := s.grpcSrv.Serve(s.listener); err != nil {
			s.errCh <- fmt.Errorf("failed to serve")
		}
		close(s.errCh)
	}()
	return nil
}
