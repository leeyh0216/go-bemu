package observability

import "fmt"

// EventKind is the small, synchronous event vocabulary emitted by the runtime.
// It deliberately does not model subscription or delivery: no asynchronous
// subscriber exists in the product, so an EventBus/outbox would add an
// unowned reliability contract.
type EventKind string

const (
	BoundaryEnter    EventKind = "boundary.enter"
	BoundaryExit     EventKind = "boundary.exit"
	BoundaryReject   EventKind = "boundary.reject"
	SideEffectBefore EventKind = "side_effect.before"
	SideEffectAfter  EventKind = "side_effect.after"
	SideEffectError  EventKind = "side_effect.error"
	DomainTransition EventKind = "domain.transition"
)

func (kind EventKind) Validate() error {
	switch kind {
	case BoundaryEnter, BoundaryExit, BoundaryReject, SideEffectBefore, SideEffectAfter, SideEffectError, DomainTransition:
		return nil
	default:
		return fmt.Errorf("unregistered observability event %q", kind)
	}
}

// Transition is the transport-neutral domain-state record. A missing
// aggregate, state, reason, or correlation ID is an invalid event rather than
// an incomplete log line.
type Transition struct {
	Aggregate     string
	From          string
	To            string
	Reason        string
	CorrelationID string
}

func NewTransition(aggregate, from, to, reason, correlationID string) (Transition, error) {
	transition := Transition{Aggregate: aggregate, From: from, To: to, Reason: reason, CorrelationID: correlationID}
	return transition, transition.Validate()
}

func (transition Transition) Validate() error {
	if transition.Aggregate == "" || transition.From == "" || transition.To == "" || transition.Reason == "" || transition.CorrelationID == "" {
		return fmt.Errorf("domain transition requires aggregate, from, to, reason, and correlation_id")
	}
	return nil
}
