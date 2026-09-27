package track

import (
    "testing"
    "time"

    "github.com/wyw14/solo-037/internal/tank"
)

func TestRequestGrantsLeaseWhenSectionFree(t *testing.T) {
    coordinator := NewCoordinator(tank.NewRegistry(2.4))
    at := time.Unix(1700000000, 0).UTC()
    acquisition, err := coordinator.Request("run-a", "section-a", "carriage-1", time.Minute, at)
    if err != nil {
        t.Fatal(err)
    }
    if !acquisition.Granted || acquisition.Lease.Run != "run-a" {
        t.Fatalf("free section not leased: %+v", acquisition)
    }
    lease, ok := coordinator.Lease("run-a")
    if !ok || lease.Fence != acquisition.Lease.Fence {
        t.Fatalf("granted lease not registered: %+v %v", lease, ok)
    }
}

func TestReleaseHandsOffAndRegistersPromotedLease(t *testing.T) {
    coordinator := NewCoordinator(tank.NewRegistry(2.4))
    at := time.Unix(1700000000, 0).UTC()
    holder, err := coordinator.Request("run-a", "section-a", "carriage-1", time.Minute, at)
    if err != nil {
        t.Fatal(err)
    }
    queued, err := coordinator.Request("run-b", "section-a", "carriage-2", time.Minute, at)
    if err != nil {
        t.Fatal(err)
    }
    if queued.Granted || queued.Position != 1 || queued.Request.Carriage != "carriage-2" {
        t.Fatalf("waiter not queued: %+v", queued)
    }
    outcome, err := coordinator.Release("run-a", at.Add(time.Second))
    if err != nil {
        t.Fatal(err)
    }
    if !outcome.Handed || outcome.Lease.Run != "run-b" || outcome.Lease.Carriage != "carriage-2" {
        t.Fatalf("handoff outcome wrong: %+v", outcome)
    }
    promoted, ok := coordinator.Lease("run-b")
    if !ok || promoted.Fence != outcome.Lease.Fence || promoted.Fence <= holder.Lease.Fence {
        t.Fatalf("promoted lease not registered: %+v %v", promoted, ok)
    }
    if _, ok := coordinator.Lease("run-a"); ok {
        t.Fatal("released holder lease still registered")
    }
    final, err := coordinator.Release("run-b", at.Add(2*time.Second))
    if err != nil {
        t.Fatal(err)
    }
    if final.Handed {
        t.Fatalf("empty queue produced a handoff: %+v", final)
    }
}

func TestWithdrawRevokesQueuedRequest(t *testing.T) {
    coordinator := NewCoordinator(tank.NewRegistry(2.4))
    at := time.Unix(1700000000, 0).UTC()
    if _, err := coordinator.Request("run-a", "section-a", "carriage-1", time.Minute, at); err != nil {
        t.Fatal(err)
    }
    if _, err := coordinator.Request("run-b", "section-a", "carriage-2", time.Minute, at); err != nil {
        t.Fatal(err)
    }
    if _, ok := coordinator.Withdraw("run-b", "section-a", "carriage-2"); !ok {
        t.Fatal("queued request not withdrawn")
    }
    if len(coordinator.Pending("section-a")) != 0 {
        t.Fatalf("withdrawn request still pending: %+v", coordinator.Pending("section-a"))
    }
    outcome, err := coordinator.Release("run-a", at.Add(time.Second))
    if err != nil {
        t.Fatal(err)
    }
    if outcome.Handed {
        t.Fatalf("withdrawn request received the section: %+v", outcome)
    }
}

func TestReleaseReportsExpiredAndKeepsSectionFree(t *testing.T) {
    coordinator := NewCoordinator(tank.NewRegistry(2.4))
    at := time.Unix(1700000000, 0).UTC()
    if _, err := coordinator.Request("run-a", "section-a", "carriage-1", time.Minute, at); err != nil {
        t.Fatal(err)
    }
    if _, err := coordinator.Request("run-b", "section-a", "carriage-2", time.Millisecond, at); err != nil {
        t.Fatal(err)
    }
    outcome, err := coordinator.Release("run-a", at.Add(time.Second))
    if err != nil {
        t.Fatal(err)
    }
    if outcome.Handed {
        t.Fatalf("expired request promoted: %+v", outcome.Lease)
    }
    if len(outcome.Expired) != 1 || outcome.Expired[0].Run != "run-b" {
        t.Fatalf("expired request not reported: %+v", outcome.Expired)
    }
    if _, ok := coordinator.Lease("run-b"); ok {
        t.Fatal("expired request registered a lease")
    }
}

func TestReleaseUnknownRunFails(t *testing.T) {
    coordinator := NewCoordinator(tank.NewRegistry(2.4))
    if _, err := coordinator.Release("missing", time.Unix(1700000000, 0).UTC()); err == nil {
        t.Fatal("release of unknown run succeeded")
    }
}
