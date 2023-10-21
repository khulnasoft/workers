// SPDX-License-Identifier: Apache-2.0
// Copyright Authors of KhulnaSoft
package workers

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrDraining is returned when an operation is not possible because
	// draining is in progress.
	ErrDraining = errors.New("drain operation in progress")
	// ErrClosed is returned when operations are attempted after a call to Close.
	ErrClosed = errors.New("workers is closed")
)

// Workers spawns, on demand, a number of worker routines to process
// submitted tasks concurrently. The number of concurrent routines never
// exceeds the specified limit.
type Workers struct {
	workers chan struct{}
	tasks   chan *task
	cancel  context.CancelFunc
	results []Task
	wg      sync.WaitGroup

	mu       sync.Mutex
	draining bool
	closed   bool
}

// New creates a new pool of workers where at most n workers process submitted
// tasks concurrently. New panics if n ≤ 0.
func New(n int) *Workers {
	return NewWithContext(context.Background(), n)
}

// NewWithContext creates a new pool of workers where at most n workers process submitted
// tasks concurrently. New panics if n ≤ 0. The context is used as the parent context to the context of the task func passed to Submit.
func NewWithContext(ctx context.Context, n int) *Workers {
	if n <= 0 {
		panic(fmt.Sprintf("workers.New: n must be > 0, got %d", n))
	}
	wp := &Workers{
		workers: make(chan struct{}, n),
		tasks:   make(chan *task),
	}
	ctx, cancel := context.WithCancel(ctx)
	wp.cancel = cancel
	go wp.run(ctx)
	return wp
}

// Cap returns the concurrent workers capacity, see New().
func (wp *Workers) Cap() int {
	return cap(wp.workers)
}

// Len returns the count of concurrent workers currently running.
func (wp *Workers) Len() int {
	return len(wp.workers)
}

// Submit submits f for processing by a worker. The given id is useful for
// identifying the task once it is completed. The task f must return when the
// context ctx is cancelled. The context passed to task f is cancelled when
// Close is called.
//
// Submit blocks until a routine start processing the task.
//
// If a drain operation is in progress, ErrDraining is returned and the task
// is not submitted for processing.
// If the workers is closed, ErrClosed is returned and the task is not
// submitted for processing.
func (wp *Workers) Submit(id string, f func(ctx context.Context) error) error {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return ErrClosed
	}
	if wp.draining {
		wp.mu.Unlock()
		return ErrDraining
	}
	wp.wg.Add(1)
	wp.mu.Unlock()
	wp.tasks <- &task{
		id:  id,
		run: f,
	}
	return nil
}

// Drain waits until all tasks are completed. This operation prevents
// submitting new tasks to the workers. Drain returns the results of the
// tasks that have been processed.
// If a drain operation is already in progress, ErrDraining is returned.
// If the workers is closed, ErrClosed is returned.
func (wp *Workers) Drain() ([]Task, error) {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return nil, ErrClosed
	}
	if wp.draining {
		wp.mu.Unlock()
		return nil, ErrDraining
	}
	wp.draining = true
	wp.mu.Unlock()

	wp.wg.Wait()

	// NOTE: It's not necessary to hold a lock when reading or writing
	// wp.results as no other routine is running at this point besides the
	// "run" routine which should be waiting on the tasks channel.
	res := wp.results
	wp.results = nil

	wp.mu.Lock()
	wp.draining = false
	wp.mu.Unlock()

	return res, nil
}

// Close closes the workers, rendering it unable to process new tasks.
// Close sends the cancellation signal to any running task and waits for all
// workers, if any, to return.
// Close will return ErrClosed if it has already been called.
func (wp *Workers) Close() error {
	wp.mu.Lock()
	if wp.closed {
		wp.mu.Unlock()
		return ErrClosed
	}
	wp.closed = true
	wp.mu.Unlock()

	wp.cancel()
	wp.wg.Wait()

	// At this point, all routines have returned. This means that Submit is not
	// pending to write to the task channel and it is thus safe to close it.
	close(wp.tasks)

	// wait for the "run" routine
	<-wp.workers
	return nil
}

// run loops over the tasks channel and starts processing routines. It should
// only be called once during the lifetime of a Workers.
func (wp *Workers) run(ctx context.Context) {
	for t := range wp.tasks {
		t := t
		result := taskResult{id: t.id}
		wp.results = append(wp.results, &result)
		wp.workers <- struct{}{}
		go func() {
			defer wp.wg.Done()
			if t.run != nil {
				result.err = t.run(ctx)
			}
			<-wp.workers
		}()
	}
	close(wp.workers)
}
