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
	"fmt"
	"sync"
)

// Queue represents a simple FIFO queue
type Queue struct {
	items []uint32
	m     sync.Mutex
}

// Enqueue adds an item to the end of the queue
func (q *Queue) Enqueue(item uint32) {
	q.m.Lock()
	defer q.m.Unlock()

	q.items = append(q.items, item)
}

// Dequeue removes and returns the first item from the queue
func (q *Queue) Dequeue() (uint32, error) {
	q.m.Lock()
	defer q.m.Unlock()

	if len(q.items) == 0 {
		return 0, fmt.Errorf("queue is empty")
	}

	item := q.items[0]
	q.items = q.items[1:]

	return item, nil
}
