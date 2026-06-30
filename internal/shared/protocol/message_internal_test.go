package protocol

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionSendWaitsForAcceptedWriteResultWhenContextCancels(t *testing.T) {
	sess := &Session{
		writes: make(chan *writeRequest, 1),
		done:   make(chan struct{}),
	}
	accepted := make(chan *writeRequest, 1)
	releaseWrite := make(chan struct{})
	go func() {
		req := <-sess.writes
		if err := req.accept(); err != nil {
			req.done <- err
			return
		}
		accepted <- req
		<-releaseWrite
		req.done <- nil
	}()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- sess.send(ctx, Message{Type: TypeCancel}, nil, false)
	}()

	select {
	case <-accepted:
	case <-time.After(time.Second):
		t.Fatal("writer did not accept send")
	}
	cancel()
	select {
	case err := <-errCh:
		t.Fatalf("send returned before accepted write completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseWrite)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("send error = %v, want accepted write result", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not return accepted write result")
	}
}

func TestSessionSendWaitsForAcceptedWriteResultWhenDoneCloses(t *testing.T) {
	sess := &Session{
		writes: make(chan *writeRequest, 1),
		done:   make(chan struct{}),
	}
	accepted := make(chan *writeRequest, 1)
	releaseWrite := make(chan struct{})
	go func() {
		req := <-sess.writes
		if err := req.accept(); err != nil {
			req.done <- err
			return
		}
		accepted <- req
		<-releaseWrite
		req.done <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- sess.send(ctx, Message{Type: TypeCancel}, nil, false)
	}()

	select {
	case <-accepted:
	case <-ctx.Done():
		t.Fatal("writer did not accept send")
	}
	close(sess.done)
	select {
	case err := <-errCh:
		t.Fatalf("send returned before accepted write completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseWrite)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("send error = %v, want accepted write result", err)
		}
	case <-ctx.Done():
		t.Fatal("send did not return accepted write result")
	}
}

func TestWriteRequestOwnershipIsSingleDecision(t *testing.T) {
	abandoned := &writeRequest{done: make(chan error, 1)}
	if !abandoned.abandon(context.Canceled) {
		t.Fatal("abandon after pending request failed")
	}
	if err := abandoned.accept(); !errors.Is(err, context.Canceled) {
		t.Fatalf("accept after abandon = %v, want context.Canceled", err)
	}
	if abandoned.acceptedByWriter {
		t.Fatal("abandoned request was marked accepted")
	}

	accepted := &writeRequest{done: make(chan error, 1)}
	if err := accepted.accept(); err != nil {
		t.Fatalf("accept pending request: %v", err)
	}
	if accepted.abandon(context.Canceled) {
		t.Fatal("abandon succeeded after writer accepted request")
	}
	if !accepted.acceptedByWriter {
		t.Fatal("accepted request was not marked accepted")
	}
}

func TestSessionWriterFailsQueuedWritesWhenDoneCloses(t *testing.T) {
	sess := &Session{
		writes: make(chan *writeRequest, 1),
		done:   make(chan struct{}),
	}
	req := &writeRequest{
		ctx:  context.Background(),
		msg:  Message{Type: TypeCancel},
		done: make(chan error, 1),
	}
	sess.writes <- req
	close(sess.done)
	sess.writer(context.Background())

	select {
	case err := <-req.done:
		if !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("queued write error = %v, want ErrSessionClosed", err)
		}
	default:
		t.Fatal("queued write was not failed when writer observed closed session")
	}
	if req.acceptedByWriter {
		t.Fatal("queued write was accepted after session closed")
	}
}
