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

func (s Startup) Launch(operation model.Operation, section string, at time.Time) (model.DeviceCommand, error) {
    lease, err := s.Tracks.Arm(operation.ID, section, operation.Carriage, at)
    if err != nil {
        return model.DeviceCommand{}, err
    }
    return s.Carriage.Accelerate(operation.ID, lease.Fence, lease.Fence, 1.2, at), nil
}

func (s Startup) Release(operation model.Operation, at time.Time) error {
    return s.Tracks.Release(operation.ID, at)
}
