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

// WaitTicket is a revocable queued request for a section lease. It is a
// handover candidate only for the exact (run, carriage) pair it holds, and
// only until ExpiresAt.
type WaitTicket struct {
    Section string `json:"section"`
    Run model.RunID `json:"run"`
    Carriage string `json:"carriage"`
    Ticket model.TicketID `json:"ticket"`
    EnqueuedAt time.Time `json:"enqueued_at"`
    ExpiresAt time.Time `json:"expires_at"`
}

func waitTicket(request tank.WaitRequest) WaitTicket {
    return WaitTicket{
        Section: request.Section,
        Run: request.Run,
        Carriage: request.Carriage,
        Ticket: request.Ticket,
        EnqueuedAt: request.EnqueuedAt,
        ExpiresAt: request.ExpiresAt,
    }
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

// Request arms the run immediately when the section is free. When another run
// holds the section, the request joins the section's waiting queue as a
// revocable WaitTicket that expires after ttl; the lease is handed over by
// Release once the current holder frees the section and the request is still
// valid at that moment.
func (c *Coordinator) Request(run model.RunID, section string, carriage string, at time.Time, ttl time.Duration) (Lease, WaitTicket, error) {
    c.mu.Lock()
    defer c.mu.Unlock()
    result, err := c.registry.Acquire(section, run, carriage, at, ttl)
    if err != nil {
        return Lease{}, WaitTicket{}, err
    }
    if !result.Granted {
        return Lease{}, waitTicket(result.Request), nil
    }
    lease := Lease{Section: section, Run: run, Carriage: carriage, Fence: result.Occupancy.Fence, LeaseID: result.Occupancy.Lease, AcquiredAt: at.UTC()}
    c.leases[run] = lease
    return lease, WaitTicket{}, nil
}

// Withdraw revokes the run's queued waiting request for the section. It
// returns false when the run has no live request queued there.
func (c *Coordinator) Withdraw(run model.RunID, section string) (WaitTicket, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    request, ok := c.registry.Withdraw(section, run)
    if !ok {
        return WaitTicket{}, false
    }
    return waitTicket(request), true
}

// Waiting lists the queued waiting tickets for a section, in arrival order.
func (c *Coordinator) Waiting(section string) []WaitTicket {
    c.mu.Lock()
    defer c.mu.Unlock()
    requests := c.registry.Waiting(section)
    result := make([]WaitTicket, 0, len(requests))
    for _, request := range requests {
        result = append(result, waitTicket(request))
    }
    return result
}

// Release frees the run's lease. When waiting candidates queue for the
// section, the lease is handed to the first candidate whose request is still
// valid at this moment; expired requests never receive the section.
func (c *Coordinator) Release(run model.RunID, at time.Time) error {
    c.mu.Lock()
    defer c.mu.Unlock()
    lease, ok := c.leases[run]
    if !ok {
        return errors.New("lease not found")
    }
    handover, released := c.registry.Release(lease.Section, run, lease.Fence, at)
    if !released {
        return errors.New("lease fence mismatch")
    }
    delete(c.leases, run)
    c.recordHandover(handover, at)
    return nil
}

func (c *Coordinator) ReleaseWithFence(run model.RunID, fence uint64, at time.Time) bool {
    c.mu.Lock()
    defer c.mu.Unlock()
    lease, ok := c.leases[run]
    if !ok || lease.Fence != fence {
        return false
    }
    handover, released := c.registry.Release(lease.Section, run, fence, at)
    if !released {
        return false
    }
    delete(c.leases, run)
    c.recordHandover(handover, at)
    return true
}

func (c *Coordinator) recordHandover(occupancy tank.Occupancy, at time.Time) {
    if occupancy.Lease == "" {
        return
    }
    c.leases[occupancy.Run] = Lease{Section: occupancy.Section, Run: occupancy.Run, Carriage: occupancy.Carriage, Fence: occupancy.Fence, LeaseID: occupancy.Lease, AcquiredAt: at.UTC()}
}

func (c *Coordinator) Lease(run model.RunID) (Lease, bool) {
    c.mu.Lock()
    defer c.mu.Unlock()
    lease, ok := c.leases[run]
    return lease, ok
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
