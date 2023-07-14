// Copyright © 2022 Meroxa, Inc.
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
	"testing"

	"github.com/matryer/is"
)

func TestQueue_Enqueue_Dequeue(t *testing.T) {
	is := is.New(t)
	queue := &Queue{}

	queue.Enqueue(1)
	queue.Enqueue(2)
	queue.Enqueue(3)

	is.Equal(len(queue.items), 3)

	item, err := queue.Dequeue()
	is.NoErr(err)
	is.Equal(item, uint32(1))
	item, err = queue.Dequeue()
	is.NoErr(err)
	is.Equal(item, uint32(2))
	item, err = queue.Dequeue()
	is.NoErr(err)
	is.Equal(item, uint32(3))

	// empty queue
	_, err = queue.Dequeue()
	is.True(err != nil)
}
