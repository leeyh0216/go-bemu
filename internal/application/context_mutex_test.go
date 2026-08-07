package application

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestContextMutexCancellationDoesNotConsumePermit(t *testing.T) {
	var mutex contextMutex
	mutex.Lock()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mutex.LockContext(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled acquisition = %v", err)
	}
	mutex.Unlock()

	ctx, cancelAcquire := context.WithTimeout(context.Background(), time.Second)
	defer cancelAcquire()
	if err := mutex.LockContext(ctx); err != nil {
		t.Fatalf("acquire after cancellation: %v", err)
	}
	mutex.Unlock()
}

func TestContextMutexDeadlineWhileHeldLeavesPermitReusable(t *testing.T) {
	var mutex contextMutex
	mutex.Lock()
	deadline, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := mutex.LockContext(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline acquisition = %v", err)
	}
	mutex.Unlock()
	reacquire, cancelReacquire := context.WithTimeout(context.Background(), time.Second)
	defer cancelReacquire()
	if err := mutex.LockContext(reacquire); err != nil {
		t.Fatalf("acquire after deadline: %v", err)
	}
	mutex.Unlock()
}
