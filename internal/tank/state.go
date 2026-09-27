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

// WaitRequest is a queued lease request for an occupied section. A request
// is identified by the trial (run) and carriage pair and expires at
// ExpiresAt; an expired request must never be granted the section.
type WaitRequest struct {
    Section string `json:"section"`
    Run model.RunID `json:"run"`
    Carriage string `json:"carriage"`
    EnqueuedAt time.Time `json:"enqueued_at"`
    ExpiresAt time.Time `json:"expires_at"`
}

func (w WaitRequest) Expired(at time.Time) bool {
    return !at.UTC().Before(w.ExpiresAt)
}

func (w WaitRequest) Matches(run model.RunID, carriage string) bool {
    return w.Run == run && w.Carriage == carriage
}

// RequestOutcome reports whether Request granted the section immediately or
// queued the caller behind the current occupant.
type RequestOutcome struct {
    Granted bool `json:"granted"`
    Occupancy Occupancy `json:"occupancy"`
    Request WaitRequest `json:"request"`
    Position int `json:"position"`
}

// Handoff reports how Release disposed of the wait queue: the first live
// candidate is promoted, expired candidates are dropped and listed so a
// stale request never silently takes the section.
type Handoff struct {
    Granted bool `json:"granted"`
    Occupancy Occupancy `json:"occupancy"`
    Expired []WaitRequest `json:"expired"`
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

// Request claims the section when it is free, otherwise appends a wait
// request identified by the (run, carriage) pair. A live queued request for
// the same run is returned unchanged; an expired or carriage-mismatched
// entry is replaced in place. Queue invariants: requests are only queued
// while another run holds the section, and Release either promotes the next
// live candidate or leaves the queue empty.
func (r *Registry) Request(section string, run model.RunID, carriage string, ttl time.Duration, at time.Time) (RequestOutcome, error) {
    r.mu.Lock()
    defer r.mu.Unlock()
    if ttl <= 0 {
        return RequestOutcome{}, errors.New("wait ttl must be positive")
    }
    now := at.UTC()
    if current, exists := r.occupancy[section]; !exists || current.Run == run {
        return RequestOutcome{Granted: true, Occupancy: r.grantLocked(section, run, carriage)}, nil
    }
    queue := r.queue[section]
    for index, pending := range queue {
        if pending.Run != run {
            continue
        }
        if !pending.Expired(now) && pending.Carriage == carriage {
            return RequestOutcome{Request: pending, Position: livePosition(queue, index, now)}, nil
        }
        request := WaitRequest{Section: section, Run: run, Carriage: carriage, EnqueuedAt: now, ExpiresAt: now.Add(ttl)}
        queue[index] = request
        r.queue[section] = queue
        return RequestOutcome{Request: request, Position: livePosition(queue, index, now)}, nil
    }
    request := WaitRequest{Section: section, Run: run, Carriage: carriage, EnqueuedAt: now, ExpiresAt: now.Add(ttl)}
    r.queue[section] = append(r.queue[section], request)
    return RequestOutcome{Request: request, Position: livePosition(r.queue[section], len(r.queue[section])-1, now)}, nil
}

// Withdraw removes a queued request; the (run, carriage) pair must match the
// queued candidate exactly. Withdrawing never disturbs the current occupant.
func (r *Registry) Withdraw(section string, run model.RunID, carriage string) (WaitRequest, bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    queue := r.queue[section]
    for index, pending := range queue {
        if !pending.Matches(run, carriage) {
            continue
        }
        r.queue[section] = append(queue[:index], queue[index+1:]...)
        return pending, true
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

// Release drops the current occupancy and, in the same critical section,
// re-checks queued candidates in order: expired requests are dropped and
// reported in Handoff.Expired, and the first live candidate is granted the
// section before Release returns.
func (r *Registry) Release(section string, run model.RunID, fence uint64, at time.Time) (Handoff, bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    current, exists := r.occupancy[section]
    if !exists || current.Run != run || current.Fence != fence {
        return Handoff{}, false
    }
    delete(r.occupancy, section)
    return r.handoffLocked(section, at.UTC()), true
}

func (r *Registry) handoffLocked(section string, now time.Time) Handoff {
    handoff := Handoff{}
    queue := r.queue[section]
    index := 0
    for index < len(queue) {
        candidate := queue[index]
        if candidate.Expired(now) {
            handoff.Expired = append(handoff.Expired, candidate)
            index++
            continue
        }
        handoff.Granted = true
        handoff.Occupancy = r.grantLocked(section, candidate.Run, candidate.Carriage)
        break
    }
    if index == len(queue) {
        delete(r.queue, section)
    } else {
        r.queue[section] = append([]WaitRequest{}, queue[index+1:]...)
    }
    return handoff
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

// Pending returns the queued wait requests for a section, oldest first.
func (r *Registry) Pending(section string) []WaitRequest {
    r.mu.Lock()
    defer r.mu.Unlock()
    return append([]WaitRequest{}, r.queue[section]...)
}

// PruneExpired drops expired requests from a section queue and returns them
// so the sweep stays visible to callers.
func (r *Registry) PruneExpired(section string, at time.Time) []WaitRequest {
    r.mu.Lock()
    defer r.mu.Unlock()
    now := at.UTC()
    kept := make([]WaitRequest, 0, len(r.queue[section]))
    var expired []WaitRequest
    for _, pending := range r.queue[section] {
        if pending.Expired(now) {
            expired = append(expired, pending)
            continue
        }
        kept = append(kept, pending)
    }
    if len(kept) == 0 {
        delete(r.queue, section)
    } else {
        r.queue[section] = kept
    }
    return expired
}

func (r *Registry) Occupancy(section string) (Occupancy, bool) {
    r.mu.Lock()
    defer r.mu.Unlock()
    current, exists := r.occupancy[section]
    return current, exists
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

// livePosition is the 1-based position of queue[index] among requests still
// live at now; expired entries ahead of it are reaped lazily and do not
// count.
func livePosition(queue []WaitRequest, index int, now time.Time) int {
    position := 1
    for i := 0; i < index; i++ {
        if !queue[i].Expired(now) {
            position++
        }
    }
    return position
}
