package tank

import (
    "testing"
    "time"
)

func TestRequestGrantsImmediatelyWhenSectionFree(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    outcome, err := registry.Request("section-a", "run-a", "carriage-1", time.Minute, at)
    if err != nil {
        t.Fatal(err)
    }
    if !outcome.Granted || outcome.Occupancy.Run != "run-a" || outcome.Occupancy.Carriage != "carriage-1" {
        t.Fatalf("free section not granted: %+v", outcome)
    }
    if len(registry.Pending("section-a")) != 0 {
        t.Fatalf("granted request left a queue entry: %+v", registry.Pending("section-a"))
    }
}

func TestRequestQueuesAndReleaseHandsOffToNextCandidate(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    holder, err := registry.Claim("section-a", "run-a", "carriage-1")
    if err != nil {
        t.Fatal(err)
    }
    first, err := registry.Request("section-a", "run-b", "carriage-2", time.Minute, at)
    if err != nil {
        t.Fatal(err)
    }
    second, err := registry.Request("section-a", "run-c", "carriage-3", time.Minute, at)
    if err != nil {
        t.Fatal(err)
    }
    if first.Granted || first.Position != 1 || second.Granted || second.Position != 2 {
        t.Fatalf("queued positions wrong: %+v %+v", first, second)
    }
    handoff, released := registry.Release("section-a", "run-a", holder.Fence, at.Add(time.Second))
    if !released || !handoff.Granted {
        t.Fatalf("release did not hand off: %v %+v", released, handoff)
    }
    if handoff.Occupancy.Run != "run-b" || handoff.Occupancy.Carriage != "carriage-2" {
        t.Fatalf("handoff went to the wrong candidate: %+v", handoff.Occupancy)
    }
    if handoff.Occupancy.Fence <= holder.Fence {
        t.Fatalf("handoff fence did not advance: %d -> %d", holder.Fence, handoff.Occupancy.Fence)
    }
    if len(handoff.Expired) != 0 {
        t.Fatalf("live candidates reported expired: %+v", handoff.Expired)
    }
    pending := registry.Pending("section-a")
    if len(pending) != 1 || pending[0].Run != "run-c" {
        t.Fatalf("remaining queue wrong: %+v", pending)
    }
    handoff, released = registry.Release("section-a", "run-b", handoff.Occupancy.Fence, at.Add(2*time.Second))
    if !released || !handoff.Granted || handoff.Occupancy.Run != "run-c" {
        t.Fatalf("second handoff wrong: %v %+v", released, handoff)
    }
}

func TestWithdrawRequiresRunAndCarriageMatch(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    holder, _ := registry.Claim("section-a", "run-a", "carriage-1")
    if _, err := registry.Request("section-a", "run-b", "carriage-2", time.Minute, at); err != nil {
        t.Fatal(err)
    }
    if _, ok := registry.Withdraw("section-a", "run-b", "carriage-9"); ok {
        t.Fatal("withdraw succeeded with a mismatched carriage")
    }
    removed, ok := registry.Withdraw("section-a", "run-b", "carriage-2")
    if !ok || removed.Run != "run-b" {
        t.Fatalf("withdraw failed: %v %+v", ok, removed)
    }
    handoff, released := registry.Release("section-a", "run-a", holder.Fence, at.Add(time.Second))
    if !released || handoff.Granted {
        t.Fatalf("withdrawn candidate received the section: %+v", handoff)
    }
    if !registry.ObserveFree("section-a") {
        t.Fatal("section still occupied after release with empty queue")
    }
}

func TestExpiredCandidateSkippedAndReportedAtHandoff(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    holder, _ := registry.Claim("section-a", "run-a", "carriage-1")
    if _, err := registry.Request("section-a", "run-b", "carriage-2", time.Millisecond, at); err != nil {
        t.Fatal(err)
    }
    if _, err := registry.Request("section-a", "run-c", "carriage-3", time.Minute, at); err != nil {
        t.Fatal(err)
    }
    handoff, released := registry.Release("section-a", "run-a", holder.Fence, at.Add(2*time.Millisecond))
    if !released || !handoff.Granted || handoff.Occupancy.Run != "run-c" {
        t.Fatalf("handoff did not skip the expired candidate: %+v", handoff)
    }
    if len(handoff.Expired) != 1 || handoff.Expired[0].Run != "run-b" {
        t.Fatalf("expired candidate not reported: %+v", handoff.Expired)
    }
}

func TestExpiredOnlyCandidateNeverOccupiesSection(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    holder, _ := registry.Claim("section-a", "run-a", "carriage-1")
    if _, err := registry.Request("section-a", "run-b", "carriage-2", time.Millisecond, at); err != nil {
        t.Fatal(err)
    }
    handoff, released := registry.Release("section-a", "run-a", holder.Fence, at.Add(time.Second))
    if !released {
        t.Fatal("release rejected")
    }
    if handoff.Granted {
        t.Fatalf("expired request silently occupied the section: %+v", handoff.Occupancy)
    }
    if len(handoff.Expired) != 1 || handoff.Expired[0].Run != "run-b" {
        t.Fatalf("expired request not surfaced: %+v", handoff.Expired)
    }
    if !registry.ObserveFree("section-a") {
        t.Fatal("section occupied after all candidates expired")
    }
    if len(registry.Pending("section-a")) != 0 {
        t.Fatalf("expired request left in queue: %+v", registry.Pending("section-a"))
    }
}

func TestRequeueIsIdempotentAndRefreshesAfterExpiry(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    _, _ = registry.Claim("section-a", "run-a", "carriage-1")
    first, _ := registry.Request("section-a", "run-b", "carriage-2", time.Minute, at)
    again, _ := registry.Request("section-a", "run-b", "carriage-2", 2*time.Minute, at.Add(time.Second))
    if again.Position != 1 || !again.Request.ExpiresAt.Equal(first.Request.ExpiresAt) {
        t.Fatalf("live re-request mutated the queue entry: %+v vs %+v", first, again)
    }
    if len(registry.Pending("section-a")) != 1 {
        t.Fatalf("duplicate queue entries: %+v", registry.Pending("section-a"))
    }
    stale, _ := registry.Request("section-a", "run-c", "carriage-3", time.Millisecond, at)
    refreshed, _ := registry.Request("section-a", "run-c", "carriage-3", time.Minute, at.Add(2*time.Millisecond))
    if refreshed.Request.ExpiresAt.Equal(stale.Request.ExpiresAt) {
        t.Fatalf("expired entry not refreshed: %+v", refreshed)
    }
    if len(registry.Pending("section-a")) != 2 {
        t.Fatalf("refresh duplicated the entry: %+v", registry.Pending("section-a"))
    }
}

func TestPruneExpiredRemovesAndReturnsStaleRequests(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    _, _ = registry.Claim("section-a", "run-a", "carriage-1")
    _, _ = registry.Request("section-a", "run-b", "carriage-2", time.Millisecond, at)
    _, _ = registry.Request("section-a", "run-c", "carriage-3", time.Minute, at)
    expired := registry.PruneExpired("section-a", at.Add(time.Second))
    if len(expired) != 1 || expired[0].Run != "run-b" {
        t.Fatalf("prune returned the wrong requests: %+v", expired)
    }
    pending := registry.Pending("section-a")
    if len(pending) != 1 || pending[0].Run != "run-c" {
        t.Fatalf("live request pruned: %+v", pending)
    }
}

func TestReleaseStillValidatesFence(t *testing.T) {
    registry := NewRegistry(2.4)
    at := time.Unix(1700000000, 0).UTC()
    holder, _ := registry.Claim("section-a", "run-a", "carriage-1")
    if _, released := registry.Release("section-a", "run-a", holder.Fence+1, at); released {
        t.Fatal("release accepted a stale fence")
    }
    if _, released := registry.Release("section-a", "run-b", holder.Fence, at); released {
        t.Fatal("release accepted a foreign run")
    }
}
