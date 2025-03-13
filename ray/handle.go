// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package ray

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
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
	stdoutLogger io.WriteCloser
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
	stdout := h.stdoutLogger
	client := rayRestClient{
		rayClusterEndpoint: h.driverConfig.RayClusterEndpoint,
	}
	ctx, cancel := context.WithTimeout(h.ctx, 2*time.Minute)
	defer cancel()

	_, err := client.DeleteActorCLI(ctx, h.ActorID)

	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			// Local timeout (2 minutes) was reached
			fmt.Fprintf(stdout, "Local timeout while deleting actor: %v\n", err)
		} else if h.ctx.Err() != nil {
			// Parent context was cancelled
			fmt.Fprintf(stdout, "Parent context cancelled while deleting actor: %v\n", h.ctx.Err())
		} else {
			// Some other error occurred
			fmt.Fprintf(stdout, "Error deleting actor: %v\n", err)
		}
	} else {
		fmt.Fprintf(stdout, "Ray actor deleted successfully - [%s]\n", h.ActorID)
	}

	return nil
}

func (h *taskHandle) run() {
	h.logger.Info("Starting run goroutine", "actor_id", h.ActorID)
	defer func() {
		h.logger.Info("Closing doneCh", "actor_id", h.ActorID)
		close(h.doneCh)
	}()

	h.stateLock.Lock()
	if h.exitResult == nil {
		h.exitResult = &drivers.ExitResult{}
	}
	h.stateLock.Unlock()

	fmt.Fprintf(h.stdoutLogger, "task handle run - %s\n", h.ActorID)
	client := rayRestClient{
		rayClusterEndpoint: h.driverConfig.RayClusterEndpoint,
	}
	h.logger.Info("Running in infinite loop", "actor_id", h.ActorID)
	for {
		fmt.Fprintf(h.stdoutLogger, "getting actor status\n")
		status, err := client.GetActorStatusCLI(h.ctx, h.ActorID)
		if err != nil {
			fmt.Fprintf(h.stdoutLogger, "Error retrieving actor status: %v\n", err)
			h.handleRunError(err, "Error retrieving actor status")
			return
		}

		fmt.Fprintf(h.stdoutLogger, "Actor Status: %s\n", status)

		if h.driverConfig.MemoryMonitoring.Enabled {
			fmt.Fprintf(h.stdoutLogger, "Fetching memory usage\n")
			memory, err := client.GetActorMemory(h.ctx, h.driverConfig.MemoryMonitoring.MetricsEndpoint, h.ActorID)
			fmt.Fprintf(h.stdoutLogger, "Current memory usage: %d\n", memory)
			if err != nil {
				fmt.Fprintf(h.stdoutLogger, "Error retrieving actor memory: %v\n", err)
			} else if memory > h.driverConfig.MemoryMonitoring.MemoryThreshold {
				fmt.Fprintf(h.stdoutLogger, "Memory usage %d MB exceeds threshold of %d MB\n",
					memory, h.driverConfig.MemoryMonitoring.MemoryThreshold)
				h.handleRunError(fmt.Errorf("memory threshold exceeded"), "Memory usage above threshold")
				return
			}
		}

		fmt.Fprintf(h.stdoutLogger, "Actor is healthy, fetching logs\n")
		actorLogs, err := client.GetActorLogsCLI(h.ctx, h.ActorID)
		if err != nil {
			fmt.Fprintf(h.stdoutLogger, "Error retrieving actor logs: %v\n", err)
			h.handleRunError(err, "Error retrieving actor logs")
			return
		}
		now := time.Now().Format(time.RFC3339)
		fmt.Fprintf(h.stdoutLogger, "[%s] Actor logs:\n%s\n", now, actorLogs)
		select {
		case <-time.After(15 * time.Second):
			fmt.Fprintf(h.stdoutLogger, "Wait of 15 seconds completed, continuing...\n")
		case <-h.ctx.Done():
			fmt.Fprintf(h.stdoutLogger, "Context cancelled, shutting down...\n")
			// h.handleRunError(h.ctx.Err(), "Context cancelled")
			return
		}
	}
}

func (h *taskHandle) handleRunError(err error, context string) {
	// Call stopTask first without holding the lock
	h.stopTask()

	h.stateLock.Lock()
	defer h.stateLock.Unlock()

	h.completedAt = time.Now()
	h.procState = drivers.TaskStateExited
	h.exitResult.ExitCode = 1
	h.exitResult.Signal = 0
	h.exitResult.Err = fmt.Errorf("%s: %v", context, err)
}

func (h *taskHandle) stop() {
	h.stateLock.Lock()
	defer h.stateLock.Unlock()

	if h.cancel != nil {
		h.cancel()
	}

	// Only update state if we're still running
	if h.procState == drivers.TaskStateRunning {
		h.completedAt = time.Now()
		h.procState = drivers.TaskStateExited
	}
}

func (h *taskHandle) closeStdoutStream() {

	if h.stdoutLogger != nil {
		time.Sleep(8 * time.Second)
		h.stateLock.Lock()
		defer h.stateLock.Unlock()
		h.stdoutLogger.Close()
		h.stdoutLogger = nil
	}
}

//TODO:
// 2. use stderr for logging errors
// 3. collect ray sterr logs
// 4. remove hard coded ray endpoints (10001 and 6379) and namespace
