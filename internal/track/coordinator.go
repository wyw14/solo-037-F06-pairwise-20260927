package track

import (
    "errors"
    "sync"
    "time"

    "github.com/wyw14/solo-037/internal/model"
    "github.com/wyw14/solo-037/internal/tank"
)

type Lease struct {
    Section string `json:"section"`
    Run model.RunID `json:"run"`
    Carriage string `json:"carriage"`
    Fence uint64 `json:"fence"`
    LeaseID model.LeaseID `json:"lease_id"`
    AcquiredAt time.Time `json:"acquired_at"`
}

// Acquisition reports whether Request leased the section immediately or
// queued the run behind the current occupant.
type Acquisition struct {
    Granted bool `json:"granted"`
    Lease Lease `json:"lease"`
    Request tank.WaitRequest `json:"request"`
    Position int `json:"position"`
}

// ReleaseOutcome reports the handoff triggered by a release: the promoted
// lease when a queued candidate took over, and the requests that expired
// while waiting.
type ReleaseOutcome struct {
    Handed bool `json:"handed"`
    Lease Lease `json:"lease"`
    Expired []tank.WaitRequest `json:"expired"`
}

type Coordinator struct {
    mu sync.Mutex
    registry *tank.Registry
    leases map[model.RunID]Lease
}

func NewCoordinator(registry *tank.Registry) *Coordinator {
    return &Coordinator{registry: registry, leases: make(map[model.RunID]Lease)}
}

func (c *Coordinator) Arm(run model.RunID, section string, carriage string, at time.Time) (Lease, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    occupancy, err := c.registry.Claim(section, run, carriage)
    if err != nil {
        return Lease{}, err
    }
    lease := Lease{Section: section, Run: run, Carriage: carriage, Fence: occupancy.Fence, LeaseID: occupancy.Lease, AcquiredAt: at.UTC()}
    c.leases[run] = lease
    return lease, nil
}

// Request arms the run when the section is free, otherwise queues a wait
// request (matched by run and carriage) that expires after ttl. A queued
// request can be revoked with Withdraw; it is promoted by the holder's
// Release only if it is still live at handoff time.
func (c *Coordinator) Request(run model.RunID, section string, carriage string, ttl time.Duration, at time.Time) (Acquisition, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    outcome, err := c.registry.Request(section, run, carriage, ttl, at)
    if err != nil {
        return Acquisition{}, err
    }
    if !outcome.Granted {
        return Acquisition{Request: outcome.Request, Position: outcome.Position}, nil
    }
    lease := Lease{Section: section, Run: run, Carriage: carriage, Fence: outcome.Occupancy.Fence, LeaseID: outcome.Occupancy.Lease, AcquiredAt: at.UTC()}
    c.leases[run] = lease
    return Acquisition{Granted: true, Lease: lease}, nil
}

// Withdraw revokes a queued wait request for the (run, carriage) pair.
func (c *Coordinator) Withdraw(run model.RunID, section string, carriage string) (tank.WaitRequest, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.registry.Withdraw(section, run, carriage)
}

func (c *Coordinator) Release(run model.RunID, at time.Time) (ReleaseOutcome, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    lease, ok := c.leases[run]
    if !ok {
        return ReleaseOutcome{}, errors.New("lease not found")
    }
    handoff, released := c.registry.Release(lease.Section, run, lease.Fence, at)
    if !released {
        return ReleaseOutcome{}, errors.New("lease fence mismatch")
    }
    delete(c.leases, run)
    return c.adopt(handoff, at), nil
}

func (c *Coordinator) ReleaseWithFence(run model.RunID, fence uint64, at time.Time) (ReleaseOutcome, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    lease, ok := c.leases[run]
    if !ok || lease.Fence != fence {
        return ReleaseOutcome{}, false
    }
    handoff, released := c.registry.Release(lease.Section, run, fence, at)
    if !released {
        return ReleaseOutcome{}, false
    }
    delete(c.leases, run)
    return c.adopt(handoff, at), true
}

// adopt registers the lease promoted by a registry handoff so the new holder
// can fence commands and release the section later.
func (c *Coordinator) adopt(handoff tank.Handoff, at time.Time) ReleaseOutcome {
    outcome := ReleaseOutcome{Expired: handoff.Expired}
    if !handoff.Granted {
        return outcome
    }
    lease := Lease{
        Section: handoff.Occupancy.Section,
        Run: handoff.Occupancy.Run,
        Carriage: handoff.Occupancy.Carriage,
        Fence: handoff.Occupancy.Fence,
        LeaseID: handoff.Occupancy.Lease,
        AcquiredAt: at.UTC(),
    }
    c.leases[lease.Run] = lease
    outcome.Handed = true
    outcome.Lease = lease
    return outcome
}

func (c *Coordinator) Lease(run model.RunID) (Lease, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    lease, ok := c.leases[run]
    return lease, ok
}

// Pending lists the wait requests queued for a section, oldest first.
func (c *Coordinator) Pending(section string) []tank.WaitRequest {
    c.mu.Lock()
    defer c.mu.Unlock()
    return c.registry.Pending(section)
}

func (c *Coordinator) Active() []Lease {
    c.mu.Lock()
    defer c.mu.Unlock()
    result := make([]Lease, 0, len(c.leases))
    for _, lease := range c.leases {
        result = append(result, lease)
    }
    return result
}
