package test

import (
    "time"

    "github.com/wyw14/solo-037/internal/carriage"
    "github.com/wyw14/solo-037/internal/model"
    "github.com/wyw14/solo-037/internal/track"
)

type Startup struct {
    Tests *Registry
    Tracks *track.Coordinator
    Carriage *carriage.Controller
}

func (s Startup) Arm(operation model.Operation, section string, at time.Time) (track.Lease, error) {
    s.Tests.Put(operation)
    return s.Tracks.Arm(operation.ID, section, operation.Carriage, at)
}

// ArmQueued arms the operation when the section is free, otherwise queues a
// revocable wait request matched by the operation's run and carriage. The
// request expires after ttl and is only promoted if still live when the
// current holder releases.
func (s Startup) ArmQueued(operation model.Operation, section string, ttl time.Duration, at time.Time) (track.Acquisition, error) {
    s.Tests.Put(operation)
    return s.Tracks.Request(operation.ID, section, operation.Carriage, ttl, at)
}

func (s Startup) Launch(operation model.Operation, section string, at time.Time) (model.DeviceCommand, error) {
    lease, err := s.Tracks.Arm(operation.ID, section, operation.Carriage, at)
    if err != nil {
        return model.DeviceCommand{}, err
    }
    return s.Carriage.Accelerate(operation.ID, lease.Fence, lease.Fence, 1.2, at), nil
}

func (s Startup) Release(operation model.Operation, at time.Time) error {
    _, err := s.Tracks.Release(operation.ID, at)
    return err
}
