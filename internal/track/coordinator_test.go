package track

import (
	"testing"
	"time"

	"github.com/wyw14/solo-037/internal/tank"
)

var leaseBase = time.Unix(1700000000, 0).UTC()

func TestRequestGrantsLeaseWhenFree(t *testing.T) {
	coordinator := NewCoordinator(tank.NewRegistry(2.4))
	lease, ticket, err := coordinator.Request("run-1", "section-a", "carriage-1", leaseBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Run != "run-1" || lease.LeaseID == "" || ticket.Ticket != "" {
		t.Fatalf("free section must grant a lease, not a ticket: %+v %+v", lease, ticket)
	}
}

func TestRequestQueuesTicketAndReleaseHandsOver(t *testing.T) {
	coordinator := NewCoordinator(tank.NewRegistry(2.4))
	holder, err := coordinator.Arm("run-1", "section-a", "carriage-1", leaseBase)
	if err != nil {
		t.Fatal(err)
	}
	lease, ticket, err := coordinator.Request("run-2", "section-a", "carriage-2", leaseBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if lease.LeaseID != "" || ticket.Ticket == "" || ticket.Carriage != "carriage-2" {
		t.Fatalf("occupied section must queue a ticket: %+v %+v", lease, ticket)
	}
	if waiting := coordinator.Waiting("section-a"); len(waiting) != 1 || waiting[0].Run != "run-2" {
		t.Fatalf("ticket must be visible in the queue: %+v", waiting)
	}
	if err := coordinator.Release("run-1", leaseBase.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	handed, ok := coordinator.Lease("run-2")
	if !ok {
		t.Fatal("handover must record a lease for the promoted run")
	}
	if handed.Carriage != "carriage-2" || handed.Fence == holder.Fence || handed.Section != "section-a" {
		t.Fatalf("handed lease does not match the queued trial and carriage: %+v", handed)
	}
	if len(coordinator.Waiting("section-a")) != 0 {
		t.Fatalf("promoted ticket must leave the queue: %+v", coordinator.Waiting("section-a"))
	}
	if err := coordinator.Release("run-2", leaseBase.Add(2*time.Second)); err != nil {
		t.Fatalf("promoted run must be able to release: %v", err)
	}
}

func TestWithdrawCancelsWaitingTicket(t *testing.T) {
	coordinator := NewCoordinator(tank.NewRegistry(2.4))
	if _, err := coordinator.Arm("run-1", "section-a", "carriage-1", leaseBase); err != nil {
		t.Fatal(err)
	}
	_, ticket, err := coordinator.Request("run-2", "section-a", "carriage-2", leaseBase, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	revoked, ok := coordinator.Withdraw("run-2", "section-a")
	if !ok || revoked.Ticket != ticket.Ticket {
		t.Fatalf("withdraw must return the revoked ticket: %+v %v", revoked, ok)
	}
	if err := coordinator.Release("run-1", leaseBase.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, ok := coordinator.Lease("run-2"); ok {
		t.Fatal("withdrawn ticket must not be handed the lease")
	}
}

func TestExpiredTicketIsNotHandedOver(t *testing.T) {
	registry := tank.NewRegistry(2.4)
	coordinator := NewCoordinator(registry)
	if _, err := coordinator.Arm("run-1", "section-a", "carriage-1", leaseBase); err != nil {
		t.Fatal(err)
	}
	if _, _, err := coordinator.Request("run-2", "section-a", "carriage-2", leaseBase, time.Second); err != nil {
		t.Fatal(err)
	}
	// The release happens after the ticket expired.
	if err := coordinator.Release("run-1", leaseBase.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, ok := coordinator.Lease("run-2"); ok {
		t.Fatal("expired ticket must not silently occupy the section")
	}
	if !registry.ObserveFree("section-a") {
		t.Fatal("section must be free once the only candidate expired")
	}
}

func TestArmStillFailsWhenOccupied(t *testing.T) {
	coordinator := NewCoordinator(tank.NewRegistry(2.4))
	if _, err := coordinator.Arm("run-1", "section-a", "carriage-1", leaseBase); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Arm("run-2", "section-a", "carriage-2", leaseBase); err == nil {
		t.Fatal("strict arm must keep failing on an occupied section")
	}
}
