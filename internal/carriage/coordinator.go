package carriage

import (
    "errors"
    "time"

    "github.com/wyw14/solo-037/internal/model"
    "github.com/wyw14/solo-037/internal/track"
)

type BrakeCoordinator struct {
    Controller *Controller
    Track *track.Coordinator
}

func NewBrakeCoordinator(controller *Controller, tracks *track.Coordinator) *BrakeCoordinator {
    return &BrakeCoordinator{Controller: controller, Track: tracks}
}

func (b *BrakeCoordinator) BrakeAndRelease(run model.RunID, commandID string, epoch uint64, at time.Time) error {
    receipt, err := b.Controller.Brake(run, commandID, epoch, at)
    if err != nil {
        return err
    }
    if !receipt.Confirmed || receipt.Velocity != 0 || !b.Controller.Stopped(run, commandID, epoch) {
        return errors.New("brake proof incomplete")
    }
    _, err = b.Track.Release(run, at)
    return err
}

func (b *BrakeCoordinator) Hold(run model.RunID) bool {
    lease, ok := b.Track.Lease(run)
    return ok && b.Controller.Velocity(run) > 0 && lease.Run == run
}
