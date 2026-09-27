package model

import (
    "fmt"
    "time"

    "github.com/google/uuid"
)

type RunID string
type RevisionID string
type EventID string
type LeaseID string
type TicketID string

type IdentitySet struct {
    Run RunID `json:"run"`
    Revision RevisionID `json:"revision"`
    Event EventID `json:"event"`
    Lease LeaseID `json:"lease"`
}

func NewRunID() RunID {
    return RunID(uuid.NewString())
}

func NewRevisionID() RevisionID {
    return RevisionID(uuid.NewString())
}

func NewEventID() EventID {
    return EventID(uuid.NewString())
}

func NewLeaseID() LeaseID {
    return LeaseID(uuid.NewString())
}

func NewTicketID() TicketID {
    return TicketID(uuid.NewString())
}

func NewIdentitySet() IdentitySet {
    return IdentitySet{
        Run: NewRunID(),
        Revision: NewRevisionID(),
        Event: NewEventID(),
        Lease: NewLeaseID(),
    }
}

func (s IdentitySet) Validate() error {
    values := []string{string(s.Run), string(s.Revision), string(s.Event), string(s.Lease)}
    for _, value := range values {
        if _, err := uuid.Parse(value); err != nil {
            return fmt.Errorf("invalid identity %q: %w", value, err)
        }
    }
    return nil
}

func (s IdentitySet) Key(at time.Time) string {
    return fmt.Sprintf("%s/%s/%d", s.Run, s.Revision, at.UTC().UnixNano())
}
