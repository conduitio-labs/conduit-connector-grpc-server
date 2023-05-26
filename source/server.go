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
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/conduitio-labs/conduit-connector-grpc-server/fromproto"
	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

type Server struct {
	pb.UnimplementedSourceServiceServer

	RecordCh    chan sdk.Record
	teardown    chan struct{}
	openContext context.Context

	stream atomic.Pointer[pb.SourceService_StreamServer]
	tomb   atomic.Pointer[*tomb.Tomb]
}

func NewServer(ctx context.Context) *Server {
	return &Server{
		RecordCh:    make(chan sdk.Record),
		teardown:    make(chan struct{}),
		openContext: ctx,
	}
}

func (s *Server) Stream(stream pb.SourceService_StreamServer) error {
	if !s.stream.CompareAndSwap(nil, &stream) {
		sdk.Logger(s.openContext).Warn().Msg("only one client connection is supported")
		return fmt.Errorf("only one client connection is supported")
	}
	t := &tomb.Tomb{}
	s.tomb.Store(&t)
	defer func() {
		s.stream.Store(nil)
	}()

	// spawn a go routine to receive records from client
	t.Go(func() error { return s.recvRecords(t, stream) })

	var err error
	select {
	case <-s.openContext.Done():
		t.Kill(nil)
		// block until teardown is called and this channel is closed
		<-s.teardown
		if !t.Alive() {
			err = t.Err()
		}
	case <-s.teardown:
		t.Kill(nil)
	case <-t.Dying():
		// wait for tomb to die
		err = t.Wait()
	}
	if err != nil {
		sdk.Logger(s.openContext).Warn().Msg(err.Error())
	}
	return err
}

func (s *Server) recvRecords(t *tomb.Tomb, stream pb.SourceService_StreamServer) error {
	for {
		record, err := stream.Recv()
		if err == io.EOF && s.openContext.Err() != nil {
			// stop signal was received
			return nil
		}
		if err != nil {
			return fmt.Errorf("error receiving record from client: %w", err)
		}
		sdkRecord, err := fromproto.Record(record)
		if err != nil {
			return err
		}
		// make sure the record channel is not closed
		select {
		case s.RecordCh <- sdkRecord:
		// worked fine!
		case <-t.Dying():
			return t.Err()
		}
	}
}

func (s *Server) SendAck(position sdk.Position) error {
	stream := s.stream.Load()
	if stream == nil {
		return fmt.Errorf("no stream is open")
	}
	err := (*stream).Send(&pb.Ack{AckPosition: position})
	if err != nil {
		return fmt.Errorf("error while sending ack into stream: %w", err)
	}
	return nil
}

func (s *Server) Close() {
	close(s.teardown)
	t := s.tomb.Load()
	if t != nil {
		_ = (*t).Wait()
	}
	close(s.RecordCh)
}
