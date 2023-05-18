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
	"context"
	"fmt"
	"sync"

	"github.com/conduitio-labs/conduit-connector-grpc-server/fromproto"
	pb "github.com/conduitio-labs/conduit-connector-grpc-server/proto/v1"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"gopkg.in/tomb.v2"
)

type Server struct {
	pb.UnimplementedStreamServiceServer

	recordCh    chan sdk.Record
	ackCh       chan sdk.Position
	ackSent     chan bool
	streamMutex sync.Mutex
	openContext context.Context
	tomb        *tomb.Tomb
	stream      pb.StreamService_StreamServer
}

func NewServer(ctx context.Context) *Server {
	return &Server{
		recordCh:    make(chan sdk.Record),
		ackCh:       make(chan sdk.Position),
		ackSent:     make(chan bool),
		openContext: ctx,
		tomb:        &tomb.Tomb{},
	}
}

func (s *Server) Stream(stream pb.StreamService_StreamServer) error {
	s.streamMutex.Lock()
	if s.stream != nil {
		s.streamMutex.Unlock()
		return fmt.Errorf("only one client connection is supported")
	}
	s.stream = stream
	s.streamMutex.Unlock()

	// spawn a go routine to receive records from client
	s.tomb.Go(s.RecvRecords)
	// spawn a go routine to send acks to the client
	s.tomb.Go(s.sendAcks)

	for {
		select {
		case <-s.openContext.Done():
			s.tomb.Kill(fmt.Errorf("open context was cancelled"))
			return s.tomb.Err()
		case <-stream.Context().Done():
			s.tomb.Kill(fmt.Errorf("stream context was cancelled"))
			return s.tomb.Err()
		case <-s.tomb.Dead():
			return s.tomb.Err()
		}
	}
}

func (s *Server) RecvRecords() error {
	defer close(s.recordCh)
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
		case s.recordCh <- sdkRecord:
			// worked fine!
		case <-s.tomb.Dying():
			return s.tomb.Err()
		}
	}
}

func (s *Server) sendAcks() error {
	defer close(s.ackCh)
	defer close(s.ackSent)
	for {
		select {
		case <-s.tomb.Dying():
			return s.tomb.Err()
		case ack, ok := <-s.ackCh:
			if !ok {
				s.ackSent <- false
				return fmt.Errorf("ack channel is closed")
			}
			err := s.stream.Send(&pb.Ack{AckPosition: ack})
			if err != nil {
				s.ackSent <- false
				return fmt.Errorf("error sending ack to stream: %w", err)
			}
			s.ackSent <- true
		}
	}
}
