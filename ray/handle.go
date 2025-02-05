// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package ray

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/client/lib/fifo"
	"github.com/hashicorp/nomad/plugins/drivers"
)

// taskHandle should store all relevant runtime information
// such as process ID if this is a local task or other meta
// data if this driver deals with external APIs
type taskHandle struct {
	// stateLock syncs access to all fields below
	stateLock sync.RWMutex

	logger       hclog.Logger
	taskConfig   *drivers.TaskConfig
	procState    drivers.TaskState
	startedAt    time.Time
	completedAt  time.Time
	exitResult   *drivers.ExitResult
	doneCh       chan struct{}
	driverConfig TaskConfig
	ActorID      string
	ctx          context.Context
	cancel       context.CancelFunc
}

func (h *taskHandle) TaskStatus() *drivers.TaskStatus {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()

	return &drivers.TaskStatus{
		ID:          h.taskConfig.ID,
		Name:        h.taskConfig.Name,
		State:       h.procState,
		StartedAt:   h.startedAt,
		CompletedAt: h.completedAt,
		ExitResult:  h.exitResult,
		DriverAttributes: map[string]string{
			"ActorID": h.ActorID,
		},
	}
}

func (h *taskHandle) IsRunning() bool {
	h.stateLock.RLock()
	defer h.stateLock.RUnlock()
	return h.procState == drivers.TaskStateRunning
}

func (h *taskHandle) stopTask() error {
	stdout, err := fifo.OpenWriter(h.taskConfig.StdoutPath)
	if err != nil {
		return fmt.Errorf("failed to open task stdout path")
	} else {

	}
	client := rayRestClient{
		rayClusterEndpoint: h.driverConfig.RayClusterEndpoint,
	}
	_, err = client.DeleteActorCLI(h.ctx, h.ActorID)
	if err != nil {
		fmt.Fprintf(stdout, "Error deleting actor: %v\n", err)
	}
	fmt.Fprintf(stdout, "remote task stopped - [%s]\n", h.ActorID)
	return nil
}

func (h *taskHandle) run() {
	defer close(h.doneCh)
	h.stateLock.Lock()
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.stateLock.Unlock()

	stdout, err := fifo.OpenWriter(h.taskConfig.StdoutPath)
	if err != nil {
		h.handleRunError(err, "failed to open task stdout path")
		return
	}
	defer func() {
		if err := stdout.Close(); err != nil {
			fmt.Fprintf(stdout, "failed to close task stdout handle correctly")
			h.logger.Error("failed to close task stdout handle correctly", "error", err)
		}
	}()
	fmt.Fprintf(stdout, "task handle run - %s\n", h.ActorID)
	client := rayRestClient{
		rayClusterEndpoint: h.driverConfig.RayClusterEndpoint,
	}

	for {
		fmt.Fprintf(stdout, "getting actor status\n")
		status, err := client.GetActorStatusCLI(h.ctx, h.ActorID)
		if err != nil {
			fmt.Fprintf(stdout, "Error retrieving actor status: %v\n", err)
			h.handleRunError(err, "Error retrieving actor status")
			return
		}

		fmt.Fprintf(stdout, "Actor Status: %s\n", status)

		if h.driverConfig.MemoryMonitoring.Enabled {
			fmt.Fprintf(stdout, "Fetching memory usage\n")
			memory, err := client.GetActorMemory(h.ctx, h.driverConfig.MemoryMonitoring.MetricsEndpoint, h.ActorID)
			fmt.Fprintf(stdout, "Current memory usage: %d\n", memory)
			if err != nil {
				fmt.Fprintf(stdout, "Error retrieving actor memory: %v\n", err)
			} else if memory > h.driverConfig.MemoryMonitoring.MemoryThreshold {
				fmt.Fprintf(stdout, "Memory usage %d MB exceeds threshold of %d MB\n",
					memory, h.driverConfig.MemoryMonitoring.MemoryThreshold)
				h.handleRunError(fmt.Errorf("memory threshold exceeded"), "Memory usage above threshold")
				return
			}
		}

		fmt.Fprintf(stdout, "Actor is healthy, fetching logs\n")
		actorLogs, err := client.GetActorLogsCLI(h.ctx, h.ActorID)
		if err != nil {
			fmt.Fprintf(stdout, "Error retrieving actor logs: %v\n", err)
			h.handleRunError(err, "Error retrieving actor logs")
			return
		}

		select {
		case <-time.After(10 * time.Second):
			now := time.Now().Format(time.RFC3339)
			fmt.Fprintf(stdout, "[%s] Actor logs:\n%s\n", now, actorLogs)
		case <-h.ctx.Done():
			fmt.Fprintf(stdout, "Context cancelled, shutting down...\n")
			// h.handleRunError(h.ctx.Err(), "Context cancelled")
			return
		}
	}
}

func (h *taskHandle) handleRunError(err error, context string) {
	h.stateLock.Lock()
	defer h.stateLock.Unlock()
	h.stopTask()
	h.completedAt = time.Now()
	h.procState = drivers.TaskStateExited
	h.exitResult.ExitCode = 1
	h.exitResult.Signal = 0
	h.exitResult.Err = fmt.Errorf("%s: %v", context, err)
	h.cancel()
}
