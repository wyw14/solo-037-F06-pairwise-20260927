package service

import (
    "context"
    "time"

    "github.com/wyw14/solo-037/internal/absorber"
    "github.com/wyw14/solo-037/internal/carriage"
    "github.com/wyw14/solo-037/internal/emergency"
    "github.com/wyw14/solo-037/internal/force"
    "github.com/wyw14/solo-037/internal/journal"
    "github.com/wyw14/solo-037/internal/model"
    "github.com/wyw14/solo-037/internal/permit"
    "github.com/wyw14/solo-037/internal/probe"
    "github.com/wyw14/solo-037/internal/tank"
    testpkg "github.com/wyw14/solo-037/internal/test"
    "github.com/wyw14/solo-037/internal/track"
    "github.com/wyw14/solo-037/internal/wave"
)

func ExerciseReachability(l *Laboratory, at time.Time) map[string]any {
    result := make(map[string]any)
    identities := model.NewIdentitySet()
    result["identity_valid"] = identities.Validate() == nil
    result["identity_key"] = identities.Key(at)
    result["stage_valid"] = model.ValidateStage(model.StageConfigured) == nil
    result["stage_sequence"] = model.StageSequence()
    evidence := model.NewEvidence(identities.Run, identities.Revision, "probe-a", model.EvidenceWaveTime, 0.01, at).WithDurability(true)
    derived := probe.DeriveSpectrum(evidence)
    result["same_source"] = evidence.SamePhysicalSource(derived)
    result["source_groups"] = probe.SameSourceGroups([]model.Evidence{evidence, derived})
    result["normalized"] = probe.Normalize([]model.Evidence{evidence, derived})
    result["revision_samples"] = probe.FilterRevision([]model.Evidence{evidence, derived}, identities.Revision)

    auxiliaryTank := tank.NewRegistry(2.1)
    result["observed_free"] = auxiliaryTank.ObserveFree("audit")
    occupancy, occupancyErr := auxiliaryTank.CommitObserved("audit", identities.Run, "audit-carriage")
    result["observed_commit"] = occupancyErr == nil
    result["observed_occupancy"] = auxiliaryTank.Snapshot()
    auxiliaryTracks := track.NewCoordinator(auxiliaryTank)
    result["lease_description"] = track.LeaseDescription(track.Lease{Section: occupancy.Section, Run: occupancy.Run, Carriage: occupancy.Carriage, Fence: occupancy.Fence, LeaseID: occupancy.Lease, AcquiredAt: at})
    _, releasedWithFence := auxiliaryTracks.ReleaseWithFence(identities.Run, occupancy.Fence, at)
    result["release_with_fence"] = releasedWithFence
    _, _ = auxiliaryTank.Release("audit", identities.Run, occupancy.Fence, at)

    sectionState := l.Sections.UpdateDepth("section-b", 2.5)
    _, sectionFound := l.Sections.Get("section-b")
    result["section_state"] = sectionState
    result["section_found"] = sectionFound
    result["sections"] = l.Sections.Snapshot()
    boundary := tank.NewBoundary()
    boundary.ResetPassive()
    result["boundary"] = boundary.State()

    generation := track.NewGeneration()
    result["generation_before"] = generation.Revision()
    result["generation_after"] = generation.Recalibrate()
    trackState := &track.State{}
    trackState.Assign(identities.Run, 8)
    trackRecovery := track.NewRecovery(auxiliaryTracks, trackState)
    result["track_restored"] = trackRecovery.Restore(identities.Run, 8)
    _ = trackRecovery.Record(identities.Run, 8, l.Store, at)

    carriageController := carriage.NewController()
    command := carriageController.Accelerate(identities.Run, 4, 8, 0.9, at)
    carriageController.SetReceipt(model.DeviceReceipt{CommandID: command.ID, Epoch: command.Epoch, Velocity: 0, Confirmed: true, RecordedAt: at})
    _, _ = carriageController.Brake(identities.Run, command.ID, command.Epoch, at)
    result["carriage_position"] = l.Position.Position()
    l.Position.UpdateRevision(model.NewRevisionID())
    result["position_window"] = l.Position.Snapshot()
    gate := carriage.NewRecoveryGate(trackState, carriageController)
    result["command_allowed"] = gate.CommandAllowed(identities.Run, 8)
    result["carriage_stopped"] = gate.Stop(identities.Run)
    worker := carriage.NewWorker()
    workerContext, workerCancel := context.WithCancel(context.Background())
    worker.Start(workerContext, identities.Run, command)
    workerCancel()
    worker.Abort()
    result["worker_active"] = worker.Active()
    result["worker_commands"] = worker.Commands()

    brakeTank := tank.NewRegistry(2.2)
    brakeTracks := track.NewCoordinator(brakeTank)
    brakeLease, _ := brakeTracks.Arm(identities.Run, "brake-section", "brake-carriage", at)
    brakeController := carriage.NewController()
    brakeCommand := brakeController.Accelerate(identities.Run, brakeLease.Fence, brakeLease.Fence, 0.5, at)
    brakeCoordinator := carriage.NewBrakeCoordinator(brakeController, brakeTracks)
    result["brake_hold"] = brakeCoordinator.Hold(identities.Run)
    result["brake_release"] = brakeCoordinator.BrakeAndRelease(identities.Run, brakeCommand.ID, brakeCommand.Epoch, at) == nil

    queueTank := tank.NewRegistry(2.3)
    queueTracks := track.NewCoordinator(queueTank)
    queueStartup := testpkg.Startup{Tests: testpkg.NewRegistry(), Tracks: queueTracks, Carriage: carriage.NewController()}
    holder := model.NewOperation("queue-holder", "model", "queue-section", "carriage-1", at)
    holderAcquisition, _ := queueStartup.ArmQueued(holder, holder.Section, time.Minute, at)
    result["queue_holder_granted"] = holderAcquisition.Granted
    stale := model.NewOperation("queue-stale", "model", "queue-section", "carriage-3", at)
    staleAcquisition, _ := queueStartup.ArmQueued(stale, stale.Section, time.Millisecond, at)
    result["queue_stale_queued"] = !staleAcquisition.Granted
    waiter := model.NewOperation("queue-waiter", "model", "queue-section", "carriage-2", at)
    waiterAcquisition, _ := queueStartup.ArmQueued(waiter, waiter.Section, time.Minute, at)
    result["queue_waiter_position"] = waiterAcquisition.Position
    withdrawn := model.NewOperation("queue-withdrawn", "model", "queue-section", "carriage-4", at)
    withdrawnAcquisition, _ := queueStartup.ArmQueued(withdrawn, withdrawn.Section, time.Minute, at)
    _, withdrawOK := queueTracks.Withdraw(withdrawn.ID, withdrawn.Section, withdrawn.Carriage)
    result["queue_withdrawn"] = !withdrawnAcquisition.Granted && withdrawOK
    handoffAt := at.Add(2 * time.Millisecond)
    handoffOutcome, handoffErr := queueTracks.Release(holder.ID, handoffAt)
    result["queue_release_error"] = errorText(handoffErr)
    result["queue_handoff_run"] = handoffOutcome.Lease.Run
    result["queue_handoff_carriage"] = handoffOutcome.Lease.Carriage
    result["queue_handoff_expired"] = handoffOutcome.Expired
    result["queue_pending"] = queueTracks.Pending("queue-section")
    late := model.NewOperation("queue-late", "model", "queue-section", "carriage-5", handoffAt)
    lateAcquisition, _ := queueStartup.ArmQueued(late, late.Section, time.Millisecond, handoffAt)
    result["queue_late_queued"] = !lateAcquisition.Granted
    finalOutcome, _ := queueTracks.Release(waiter.ID, handoffAt.Add(2*time.Millisecond))
    result["queue_final_handed"] = finalOutcome.Handed
    result["queue_final_expired"] = finalOutcome.Expired
    result["queue_section_free"] = queueTank.ObserveFree("queue-section")

    forceReceiver := force.NewReceiver()
    result["force_persist"] = force.Persist(forceReceiver, l.Store, identities.Run, identities.Revision, 3.5, at) == nil
    latestForce, latestFound := forceReceiver.Latest(identities.Run)
    result["latest_force"] = latestForce
    result["latest_force_found"] = latestFound
    result["force_complete"] = force.Complete(forceReceiver, identities.Run)

    leftRecovery := journal.RecoveryDecision{Run: identities.Run, Stage: model.StageSettling, Fence: 7, EvidenceReady: true, CanClear: false}
    rightRecovery := journal.RecoveryDecision{Run: identities.Run, Stage: model.StageComplete, Fence: 8, EvidenceReady: true, CanClear: true}
    result["merged_recovery"] = journal.MergeRecovery(leftRecovery, rightRecovery)
    snapshot := l.Snapshots.Build(l.Store.Records(), at)
    result["snapshot_decision"] = journal.SnapshotDecision(snapshot, identities.Run)
    result["merged_snapshot"] = journal.MergeSnapshots(snapshot, snapshot)
    l.Store.FailNext("")

    evaluator := permit.NewEvaluator()
    evaluator.SetSources(identities.Run, 2)
    evaluator.SetCalm(identities.Run, true)
    result["source_count"] = evaluator.SourceCount(identities.Run)
    evaluator.ObserveBrake(identities.Run, model.DeviceReceipt{CommandID: command.ID, Epoch: command.Epoch, Velocity: 0, Confirmed: true})
    result["bound_brake"] = permit.BindBrake(evaluator, carriageController, identities.Run, command.ID, command.Epoch)
    result["synced_sources"] = permit.SyncWaveSources(evaluator, identities.Run, 2, true)
    permitRecovery := permit.Restore(model.StageComplete, evaluator.Evaluate(identities.Run))
    result["permit_recovery"] = permitRecovery.Description()

    probeCoordinator := probe.NewCoordinator(identities.Run, identities.Revision, time.Second)
    result["record_to_section"] = probe.RecordToSection(probeCoordinator, l.Sections, "section-b", identities.Run, identities.Revision, "probe-a", 0.01, at)
    probe.ResetAfterGap(probeCoordinator, identities.Revision)
    result["probe_calm"] = probeCoordinator.IsCalm()
    probeCoordinator.Collection.Invalidate(identities.Run, identities.Revision)

    waveController := wave.NewController()
    waveContext, waveCancel := context.WithCancel(context.Background())
    wave.StartForRun(waveContext, identities.Run, identities.Revision, waveController, at)
    waveCancel()
    result["wave_stopped"] = waveController.WaitStopped(time.Second)
    wave.StopForAbort(waveController)
    result["wave_commands"] = wave.CommandCount(waveController, identities.Run)

    absorberController := absorber.NewController(tank.NewBoundary())
    absorberCoordinator := absorber.NewCoordinator(absorberController)
    result["absorber_configured"] = absorberCoordinator.ConfigureActive(at) == nil
    result["reflection_observed"] = absorberCoordinator.ObserveReflection(identities.Run, identities.Revision, 3)
    result["reflection_ready"] = absorberCoordinator.Ready(identities.Run, identities.Revision)
    result["absorber_reactivated"] = absorber.ActivateAfterBoundary(absorberController, at) == nil

    emergencyCoordinator := emergency.NewCoordinator(emergency.NewBrake(carriageController, waveController))
    emergencyCoordinator.Stop(identities.Run, command.ID, command.Epoch, nil, at)
    result["emergency_secured"] = emergencyCoordinator.Secured(identities.Run)

    operation := model.NewOperation("reachability", "model", "reach-section", "reach-carriage", at)
    testCoordinator := testpkg.NewCoordinator(operation, l.Store, probe.NewCollection(), force.NewReceiver())
    _ = testCoordinator.Arm(at)
    _ = testCoordinator.BeginMeasurement(at)
    testCoordinator.ResetContext()
    result["test_snapshot"] = testCoordinator.Snapshot()
    startupTank := tank.NewRegistry(2)
    startupTracks := track.NewCoordinator(startupTank)
    startupCarriage := carriage.NewController()
    startup := testpkg.Startup{Tests: testpkg.NewRegistry(), Tracks: startupTracks, Carriage: startupCarriage}
    launched, launchErr := startup.Launch(operation, operation.Section, at)
    result["startup_launch"] = launched
    result["startup_launch_error"] = errorText(launchErr)
    result["startup_release"] = startup.Release(operation, at) == nil
    result["can_remove_model"] = testpkg.CanRemoveModel(l.Store.Records(), identities.Run)
    result["evidence_count"] = testpkg.EvidenceCount(l.Store.Records(), identities.Run)

    runtimeRecovery := Recovery{Journal: l.Store, Track: trackState, Carriage: gate}
    result["recovery_steps"] = runtimeRecovery.Steps(identities.Run)
    return result
}
