package tank

import (
	"testing"
	"time"

	"github.com/wyw14/solo-037/internal/model"
)

var queueBase = time.Unix(1700000000, 0).UTC()

func occupy(t *testing.T, registry *Registry, section string, run model.RunID, carriage string) Occupancy {
	t.Helper()
	occupancy, err := registry.Claim(section, run, carriage)
	if err != nil {
		t.Fatalf("claim %s: %v", section, err)
	}
	return occupancy
}

func TestAcquireGrantsImmediatelyWhenFree(t *testing.T) {
	registry := NewRegistry(2.4)
	result, err := registry.Acquire("section-a", "run-1", "carriage-1", queueBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Granted || result.Occupancy.Run != "run-1" || result.Occupancy.Carriage != "carriage-1" {
		t.Fatalf("expected immediate grant, got %+v", result)
	}
	if len(registry.Waiting("section-a")) != 0 {
		t.Fatalf("no queue expected: %+v", registry.Waiting("section-a"))
	}
}

func TestAcquireQueuesWhenOccupied(t *testing.T) {
	registry := NewRegistry(2.4)
	occupy(t, registry, "section-a", "run-1", "carriage-1")
	result, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if result.Granted {
		t.Fatalf("occupied section must not grant: %+v", result)
	}
	request := result.Request
	if request.Run != "run-2" || request.Carriage != "carriage-2" || request.Ticket == "" {
		t.Fatalf("queued request incomplete: %+v", request)
	}
	if !request.ExpiresAt.Equal(queueBase.Add(time.Minute)) {
		t.Fatalf("expiry not ttl-based: %+v", request)
	}
	waiting := registry.Waiting("section-a")
	if len(waiting) != 1 || waiting[0].Ticket != request.Ticket {
		t.Fatalf("queue does not hold the request: %+v", waiting)
	}
}

func TestAcquireRejectsNonPositiveTTL(t *testing.T) {
	registry := NewRegistry(2.4)
	occupy(t, registry, "section-a", "run-1", "carriage-1")
	if _, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, 0); err == nil {
		t.Fatal("zero ttl must be rejected, not silently queued")
	}
	if len(registry.Waiting("section-a")) != 0 {
		t.Fatalf("rejected request must not linger: %+v", registry.Waiting("section-a"))
	}
}

func TestReleaseHandsOverToNextValidCandidate(t *testing.T) {
	registry := NewRegistry(2.4)
	holder := occupy(t, registry, "section-a", "run-1", "carriage-1")
	if _, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, time.Minute); err != nil {
		t.Fatal(err)
	}
	handover, released := registry.Release("section-a", "run-1", holder.Fence, queueBase.Add(time.Second))
	if !released {
		t.Fatal("release with matching fence must succeed")
	}
	if handover.Run != "run-2" || handover.Carriage != "carriage-2" || handover.Lease == "" {
		t.Fatalf("handover did not match the queued trial and carriage: %+v", handover)
	}
	if handover.Fence == holder.Fence {
		t.Fatalf("handover must fence a fresh lease: %+v", handover)
	}
	if len(registry.Waiting("section-a")) != 0 {
		t.Fatalf("promoted request must leave the queue: %+v", registry.Waiting("section-a"))
	}
}

func TestReleaseRechecksExpiryAndSkipsExpiredCandidates(t *testing.T) {
	registry := NewRegistry(2.4)
	holder := occupy(t, registry, "section-a", "run-1", "carriage-1")
	if _, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Acquire("section-a", "run-3", "carriage-3", queueBase, time.Minute); err != nil {
		t.Fatal(err)
	}
	// run-2's request expired one second in; the release happens after that.
	handover, released := registry.Release("section-a", "run-1", holder.Fence, queueBase.Add(30*time.Second))
	if !released {
		t.Fatal("release must succeed")
	}
	if handover.Run != "run-3" || handover.Carriage != "carriage-3" {
		t.Fatalf("expired candidate must be skipped: %+v", handover)
	}
	if occupancy, exists := registry.Occupancy("section-a"); !exists || occupancy.Run != "run-3" {
		t.Fatalf("section must be held by run-3: %+v", occupancy)
	}
}

func TestExpiredRequestNeverOccupiesSection(t *testing.T) {
	registry := NewRegistry(2.4)
	holder := occupy(t, registry, "section-a", "run-1", "carriage-1")
	if _, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, time.Second); err != nil {
		t.Fatal(err)
	}
	handover, released := registry.Release("section-a", "run-1", holder.Fence, queueBase.Add(time.Hour))
	if !released {
		t.Fatal("release must succeed")
	}
	if handover.Lease != "" {
		t.Fatalf("expired request silently occupied the section: %+v", handover)
	}
	if !registry.ObserveFree("section-a") {
		t.Fatal("section must be free after the only candidate expired")
	}
	if len(registry.Waiting("section-a")) != 0 {
		t.Fatalf("expired request must be discarded: %+v", registry.Waiting("section-a"))
	}
}

func TestWithdrawRevokesWaitingRequest(t *testing.T) {
	registry := NewRegistry(2.4)
	holder := occupy(t, registry, "section-a", "run-1", "carriage-1")
	queued, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	revoked, ok := registry.Withdraw("section-a", "run-2")
	if !ok || revoked.Ticket != queued.Request.Ticket {
		t.Fatalf("withdraw must return the revoked request: %+v %v", revoked, ok)
	}
	if _, ok := registry.Withdraw("section-a", "run-2"); ok {
		t.Fatal("withdrawn request must not be revocable twice")
	}
	handover, released := registry.Release("section-a", "run-1", holder.Fence, queueBase.Add(time.Second))
	if !released || handover.Lease != "" {
		t.Fatalf("withdrawn request must not be handed over: %+v", handover)
	}
	if !registry.ObserveFree("section-a") {
		t.Fatal("section must be free")
	}
}

func TestQueuedCandidateMatchedByRunAndCarriage(t *testing.T) {
	registry := NewRegistry(2.4)
	holder := occupy(t, registry, "section-a", "run-1", "carriage-1")
	first, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// Same trial and carriage: the existing ticket is refreshed, not duplicated.
	refreshed, err := registry.Acquire("section-a", "run-2", "carriage-2", queueBase.Add(10*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed.Request.Ticket != first.Request.Ticket {
		t.Fatalf("matching re-request must keep its ticket: %+v", refreshed.Request)
	}
	if !refreshed.Request.ExpiresAt.Equal(queueBase.Add(10 * time.Second).Add(time.Minute)) {
		t.Fatalf("matching re-request must refresh expiry: %+v", refreshed.Request)
	}
	// Same trial, different carriage: the stale candidate is replaced.
	replaced, err := registry.Acquire("section-a", "run-2", "carriage-9", queueBase.Add(20*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Request.Ticket == first.Request.Ticket || replaced.Request.Carriage != "carriage-9" {
		t.Fatalf("carriage change must replace the candidate: %+v", replaced.Request)
	}
	if waiting := registry.Waiting("section-a"); len(waiting) != 1 {
		t.Fatalf("one candidate per trial expected: %+v", waiting)
	}
	handover, released := registry.Release("section-a", "run-1", holder.Fence, queueBase.Add(30*time.Second))
	if !released || handover.Carriage != "carriage-9" {
		t.Fatalf("handover must use the latest trial and carriage match: %+v", handover)
	}
}

func TestReleaseWithStaleFenceKeepsOccupancy(t *testing.T) {
	registry := NewRegistry(2.4)
	holder := occupy(t, registry, "section-a", "run-1", "carriage-1")
	if _, released := registry.Release("section-a", "run-1", holder.Fence+1, queueBase); released {
		t.Fatal("stale fence must not release")
	}
	if _, released := registry.Release("section-a", "run-2", holder.Fence, queueBase); released {
		t.Fatal("foreign run must not release")
	}
	if registry.ObserveFree("section-a") {
		t.Fatal("occupancy must survive failed releases")
	}
}
