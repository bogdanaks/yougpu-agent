package agent

import (
	"context"
	"testing"
)

func TestAliveContextStaysCancelledAfterDeletionRequested(t *testing.T) {
	a := &Agent{}
	a.stopAlive()
	if err := a.aliveContext(context.Background()).Err(); err == nil {
		t.Fatal("alive work started after deletion was requested")
	}
}

func TestAliveContextSurvivesUntilDeletion(t *testing.T) {
	a := &Agent{}
	ctx := a.aliveContext(context.Background())
	if ctx.Err() != nil {
		t.Fatal("fresh alive context is cancelled")
	}
	if a.aliveContext(context.Background()) != ctx {
		t.Fatal("alive context is not reused")
	}
	a.stopAlive()
	if ctx.Err() == nil {
		t.Fatal("deletion did not cancel alive work")
	}
}
