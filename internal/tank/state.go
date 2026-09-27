package tank

import (
    "errors"
    "sync"
    "time"

    "github.com/wyw14/solo-037/internal/model"
)

type Occupancy struct {
    Section string `json:"section"`
    Run model.RunID `json:"run"`
    Carriage string `json:"carriage"`
    Lease model.LeaseID `json:"lease"`
    Fence uint64 `json:"fence"`
}

// WaitRequest is a revocable queued claim on an occupied section. It is only a
// candidate for the exact (run, carriage) pair it was made for, and it stops
// being a candidate once ExpiresAt has passed.
type WaitRequest struct {
    Section string `json:"section"`
    Run model.RunID `json:"run"`
    Carriage string `json:"carriage"`
    Ticket model.TicketID `json:"ticket"`
    EnqueuedAt time.Time `json:"enqueued_at"`
    ExpiresAt time.Time `json:"expires_at"`
}

func (w WaitRequest) Expired(at time.Time) bool {
    return !at.UTC().Before(w.ExpiresAt)
}

// AcquireResult is the outcome of Acquire: either the section was granted
// immediately (Granted, Occupancy set) or the request was queued (Request set).
type AcquireResult struct {
    Granted bool
    Occupancy Occupancy
    Request WaitRequest
}

type Registry struct {
    mu sync.Mutex
    nextFence uint64
    occupancy map[string]Occupancy
    queue map[string][]WaitRequest
    depthRevision model.RevisionID
    depthMeters float64
}

func NewRegistry(depth float64) *Registry {
    return &Registry{
        occupancy: make(map[string]Occupancy),
        queue: make(map[string][]WaitRequest),
        depthRevision: model.NewRevisionID(),
        depthMeters: depth,
    }
}

func (r *Registry) Claim(section string, run model.RunID, carriage string) (Occupancy, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    if current, exists := r.occupancy[section]; exists && current.Run != run {
        return Occupancy{}, errors.New("section occupied")
    }
    return r.grantLocked(section, run, carriage), nil
}

// Acquire grants the section lease immediately when it is free (or already
// held by the same run). When another run holds the section, the request is
// queued as a revocable WaitRequest that expires after ttl; the lease is then
// handed over by Release once the current holder frees the section.
func (r *Registry) Acquire(section string, run model.RunID, carriage string, at time.Time, ttl time.Duration) (AcquireResult, error) {
    if ttl <= 0 {
        return AcquireResult{}, errors.New("wait ttl must be positive")
    }
    r.mu.Lock()
    defer r.mu.Unlock()
    at = at.UTC()
    r.pruneLocked(section, at)
    if current, exists := r.occupancy[section]; !exists || current.Run == run {
        return AcquireResult{Granted: true, Occupancy: r.grantLocked(section, run, carriage)}, nil
    }
    queue := r.queue[section]
    for index, queued := range queue {
        if queued.Run != run {
            continue
        }
        if queued.Carriage == carriage {
            queued.ExpiresAt = at.Add(ttl)
            queue[index] = queued
            return AcquireResult{Request: queued}, nil
        }
        // Same trial, different carriage: the stale candidate is replaced so a
        // mismatched carriage is never handed the section.
        request := WaitRequest{Section: section, Run: run, Carriage: carriage, Ticket: model.NewTicketID(), EnqueuedAt: at, ExpiresAt: at.Add(ttl)}
        queue[index] = request
        return AcquireResult{Request: request}, nil
    }
    request := WaitRequest{Section: section, Run: run, Carriage: carriage, Ticket: model.NewTicketID(), EnqueuedAt: at, ExpiresAt: at.Add(ttl)}
    r.queue[section] = append(queue, request)
    return AcquireResult{Request: request}, nil
}

// Withdraw revokes a queued waiting request. It returns the revoked request;
// a run that no longer queues (never queued, already granted, or expired) gets false.
func (r *Registry) Withdraw(section string, run model.RunID) (WaitRequest, bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    queue := r.queue[section]
    for index, request := range queue {
        if request.Run == run {
            r.queue[section] = append(queue[:index], queue[index+1:]...)
            return request, true
        }
    }
    return WaitRequest{}, false
}

func (r *Registry) ObserveFree(section string) bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    _, exists := r.occupancy[section]
    return !exists
}

func (r *Registry) CommitObserved(section string, run model.RunID, carriage string) (Occupancy, error) {
    return r.Claim(section, run, carriage)
}

// Release removes the run's occupancy and hands the section to the next
// waiting candidate. Conditions are re-checked at handover time: expired
// requests are discarded and never occupy the section. The handed-over
// occupancy is returned (zero value when no candidate survived the check).
func (r *Registry) Release(section string, run model.RunID, fence uint64, at time.Time) (Occupancy, bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    current, exists := r.occupancy[section]
    if !exists || current.Run != run || current.Fence != fence {
        return Occupancy{}, false
    }
    delete(r.occupancy, section)
    handover, _ := r.promoteLocked(section, at.UTC())
    return handover, true
}

func (r *Registry) Occupancy(section string) (Occupancy, bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    current, exists := r.occupancy[section]
    return current, exists
}

// Waiting returns a copy of the queued requests for a section, in arrival order.
func (r *Registry) Waiting(section string) []WaitRequest {
    r.mu.Lock()
    defer r.mu.Unlock()
    queue := r.queue[section]
    result := make([]WaitRequest, len(queue))
    copy(result, queue)
    return result
}

// QueueSnapshot returns a copy of every queued request across all sections.
func (r *Registry) QueueSnapshot() []WaitRequest {
    r.mu.Lock()
    defer r.mu.Unlock()
    result := make([]WaitRequest, 0)
    for _, queue := range r.queue {
        result = append(result, queue...)
    }
    return result
}

func (r *Registry) Snapshot() []Occupancy {
    r.mu.Lock()
    defer r.mu.Unlock()
    result := make([]Occupancy, 0, len(r.occupancy))
    for _, occupancy := range r.occupancy {
        result = append(result, occupancy)
    }
    return result
}

func (r *Registry) grantLocked(section string, run model.RunID, carriage string) Occupancy {
    r.nextFence++
    occupancy := Occupancy{
        Section: section,
        Run: run,
        Carriage: carriage,
        Lease: model.NewLeaseID(),
        Fence: r.nextFence,
    }
    r.occupancy[section] = occupancy
    return occupancy
}

// promoteLocked grants the section to the first queued candidate that is still
// valid at handover time. Expired requests are dropped, never granted.
func (r *Registry) promoteLocked(section string, at time.Time) (Occupancy, bool) {
    queue := r.queue[section]
    for len(queue) > 0 {
        candidate := queue[0]
        queue = queue[1:]
        if candidate.Expired(at) {
            continue
        }
        r.queue[section] = queue
        return r.grantLocked(section, candidate.Run, candidate.Carriage), true
    }
    delete(r.queue, section)
    return Occupancy{}, false
}

// pruneLocked discards expired requests so they cannot linger in the queue.
func (r *Registry) pruneLocked(section string, at time.Time) {
    queue := r.queue[section]
    kept := make([]WaitRequest, 0, len(queue))
    for _, request := range queue {
        if !request.Expired(at) {
            kept = append(kept, request)
        }
    }
    if len(kept) == 0 {
        delete(r.queue, section)
        return
    }
    r.queue[section] = kept
}
