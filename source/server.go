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
	"sync"

	"github.com/conduitio-labs/conduit-connector-grpc-server/fromproto"
	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

type Server struct {
	pb.UnimplementedSourceServiceServer

	RecordCh    chan sdk.Record
	Teardown    chan bool
	streamMutex sync.Mutex
	openContext context.Context
	tomb        *tomb.Tomb
	stream      pb.SourceService_StreamServer
}

func NewServer(ctx context.Context) *Server {
	return &Server{
		RecordCh: make(chan sdk.Record),
		// buffering channel, so Teardown won't be blocked until the signal is received
		Teardown:    make(chan bool, 1),
		openContext: ctx,
		tomb:        &tomb.Tomb{},
	}
}

func (s *Server) Stream(stream pb.SourceService_StreamServer) error {
	s.streamMutex.Lock()
	if s.stream != nil {
		s.streamMutex.Unlock()
		return fmt.Errorf("only one client connection is supported")
	}
	s.stream = stream
	s.streamMutex.Unlock()

	// spawn a go routine to receive records from client
	s.tomb.Go(s.recvRecords)

	select {
	case <-s.openContext.Done():
		s.tomb.Kill(fmt.Errorf("open context was cancelled"))
		// wait for Teardown to send a signal that it's called
		<-s.Teardown
	case <-stream.Context().Done():
		s.tomb.Kill(fmt.Errorf("stream context was cancelled"))
	case <-s.tomb.Dying():
		// wait for tomb to die
	}
	return s.tomb.Wait()
}

func (s *Server) recvRecords() error {
	defer close(s.RecordCh)
	for {
		record, err := s.stream.Recv()
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
		case <-s.tomb.Dying():
			return s.tomb.Err()
		}
	}
}

func (s *Server) SendAck(position sdk.Position) error {
	err := s.stream.Send(&pb.Ack{AckPosition: position})
	if err != nil {
		return fmt.Errorf("error while sending ack into stream: %w", err)
	}
	return nil
}
