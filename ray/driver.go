// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package ray

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/client/lib/fifo"
	"github.com/hashicorp/nomad/client/structs"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	nstructs "github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
)

const (
	// pluginName is the name of the plugin
	// this is used for logging and (along with the version) for uniquely
	// identifying plugin binaries fingerprinted by the client
	pluginName = "ray"

	// pluginVersion allows the client to identify and use newer versions of
	// an installed plugin
	pluginVersion = "v0.1.0"

	// fingerprintPeriod is the interval at which the plugin will send
	// fingerprint responses
	fingerprintPeriod = 30 * time.Second

	// taskHandleVersion is the version of task handle which this plugin sets
	// and understands how to decode
	// this is used to allow modification and migration of the task schema
	// used by the plugin
	taskHandleVersion = 1
)

var (
	// pluginInfo describes the plugin
	pluginInfo = &base.PluginInfoResponse{
		Type:              base.PluginTypeDriver,
		PluginApiVersions: []string{drivers.ApiVersion010},
		PluginVersion:     pluginVersion,
		Name:              pluginName,
	}

	// configSpec is the specification of the plugin's configuration
	// this is used to validate the configuration specified for the plugin
	// on the client.
	// this is not global, but can be specified on a per-client basis.
	configSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		// TODO: define plugin's agent configuration schema.
		//
		// The schema should be defined using HCL specs and it will be used to
		// validate the agent configuration provided by the user in the
		// `plugin` stanza (https://www.nomadproject.io/docs/configuration/plugin.html).
		//
		// For example, for the schema below a valid configuration would be:
		//
		//   plugin "hello-driver-plugin" {
		//     config {
		//       shell = "fish"
		//     }
		//   }
		"ray_cluster_endpoint": hclspec.NewDefault(
			hclspec.NewAttr("ray_cluster_endpoint", "string", false),
			hclspec.NewLiteral(`"http://localhost:8265"`),
		),
		"enabled": hclspec.NewDefault(
			hclspec.NewAttr("enabled", "bool", false),
			hclspec.NewLiteral(`false`),
		),
	})

	// taskConfigSpec is the specification of the plugin's configuration for
	// a task
	// this is used to validated the configuration specified for the plugin
	// when a job is submitted.
	taskConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		// TODO: define plugin's task configuration schema
		//
		// The schema should be defined using HCL specs and it will be used to
		// validate the task configuration provided by the user when they
		// submit a job.
		//
		// For example, for the schema below a valid task would be:
		//   job "example" {
		//     group "example" {
		//       task "say-hi" {
		//         driver = "hello-driver-plugin"
		//         config {
		//           greeting = "Hi"
		//         }
		//       }
		//     }
		//   }
		"ray_cluster_endpoint": hclspec.NewDefault(
			hclspec.NewAttr("ray_cluster_endpoint", "string", true),
			hclspec.NewLiteral(`"http://localhost:8265"`),
		),
		"ray_api_endpoint": hclspec.NewDefault(
			hclspec.NewAttr("ray_api_endpoint", "string", true),
			hclspec.NewLiteral(`"http://localhost:8000"`),
		),
		"namespace": hclspec.NewDefault(
			hclspec.NewAttr("namespace", "string", false),
			hclspec.NewLiteral(`"default"`),
		),
		"max_actor_restarts": hclspec.NewDefault(
			hclspec.NewAttr("max_actor_restarts", "number", false),
			hclspec.NewLiteral(`0`),
		),
		"max_task_retries": hclspec.NewDefault(
			hclspec.NewAttr("max_task_retries", "number", false),
			hclspec.NewLiteral(`20`),
		),
		"num_cpus": hclspec.NewDefault(
			hclspec.NewAttr("num_cpus", "number", false),
			hclspec.NewLiteral(`0.5`),
		),
		"memory_monitoring":  hclspec.NewBlock("memory_monitoring", false, memoryMonitoringConfigSpec),
		"pipeline_file_path": hclspec.NewAttr("pipeline_file_path", "string", true),
		"pipeline_runner":    hclspec.NewAttr("pipeline_runner", "string", true),
		"actor_name":         hclspec.NewAttr("actor_name", "string", true),
	})

	memoryMonitoringConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
		"enabled":          hclspec.NewAttr("enabled", "bool", false),
		"metrics_endpoint": hclspec.NewAttr("metrics_endpoint", "string", false),
		"memory_threshold": hclspec.NewAttr("memory_threshold", "number", false),
	})
	// capabilities indicates what optional features this driver supports
	// this should be set according to the target run time.
	capabilities = &drivers.Capabilities{
		// The plugin's capabilities signal Nomad which extra functionalities
		// are supported. For a list of available options check the docs page:
		// https://godoc.org/github.com/hashicorp/nomad/plugins/drivers#Capabilities
		//TODO: find references to implement signals properly
		SendSignals: false,
		Exec:        false,
		RemoteTasks: true,
	}
)

// Config contains configuration information for the plugin
type Config struct {
	// This struct is the decoded version of the schema defined in the
	// configSpec variable above. It's used to convert the HCL configuration
	// passed by the Nomad agent into Go contructs.
	Enabled            bool   `codec:"enabled"`
	RayClusterEndpoint string `codec:"ray_cluster_endpoint"`
}

// TaskConfig contains configuration information for a task that runs with
// this plugin
type TaskConfig struct {
	// This struct is the decoded version of the schema defined in the
	// taskConfigSpec variable above. It's used to convert the string
	// configuration for the task into Go contructs.
	Namespace          string                 `codec:"namespace"`
	RayClusterEndpoint string                 `codec:"ray_cluster_endpoint"`
	RayServeEndpoint   string                 `codec:"ray_api_endpoint"`
	MemoryMonitoring   MemoryMonitoringConfig `codec:"memory_monitoring"`
	MaxActorRestarts   int64                  `codec:"max_actor_restarts"`
	NumCpu             float64                `codec:"num_cpus"`
	MaxTaskRetries     int64                  `codec:"max_task_retries"`
	PipelineFilePath   string                 `codec:"pipeline_file_path"`
	PipelineRunner     string                 `codec:"pipeline_runner"`
	ActorName          string                 `codec:"actor_name"`
}

type MemoryMonitoringConfig struct {
	// This struct is the decoded version of the schema defined in the
	// taskConfigSpec variable above. It's used to convert the string
	// configuration for the task into Go contructs.
	MetricsEndpoint string `codec:"metrics_endpoint"`
	Enabled         bool   `codec:"enabled"`
	MemoryThreshold int64  `codec:"memory_threshold"`
}

// TaskState is the runtime state which is encoded in the handle returned to
// Nomad client.
// This information is needed to rebuild the task state and handler during
// recovery.
type TaskState struct {
	TaskConfig *drivers.TaskConfig
	StartedAt  time.Time

	// The plugin keeps track of its running tasks in a in-memory data
	// structure. If the plugin crashes, this data will be lost, so Nomad
	// will respawn a new instance of the plugin and try to restore its
	// in-memory representation of the running tasks using the RecoverTask()
	// method below.
	ActorID string
}

// RayDriverPlugin is an example driver plugin. When provisioned in a job,
// the taks will output a greet specified by the user.
type RayDriverPlugin struct {
	// eventer is used to handle multiplexing of TaskEvents calls such that an
	// event can be broadcast to all callers
	eventer *eventer.Eventer

	// config is the plugin configuration set by the SetConfig RPC
	config *Config

	// nomadConfig is the client config from Nomad
	nomadConfig *base.ClientDriverConfig

	// tasks is the in memory datastore mapping taskIDs to driver handles
	tasks *taskStore

	// ctx is the context for the driver. It is passed to other subsystems to
	// coordinate shutdown
	ctx context.Context

	// signalShutdown is called when the driver is shutting down and cancels
	// the ctx passed to any subsystems
	signalShutdown context.CancelFunc

	// logger will log to the Nomad agent
	logger hclog.Logger

	client rayRestInterface
}

// NewPlugin returns a new example driver plugin
func NewPlugin(logger hclog.Logger) drivers.DriverPlugin {
	ctx, cancel := context.WithCancel(context.Background())
	logger = logger.Named(pluginName)

	return &RayDriverPlugin{
		eventer:        eventer.NewEventer(ctx, logger),
		config:         &Config{},
		tasks:          newTaskStore(),
		ctx:            ctx,
		signalShutdown: cancel,
		logger:         logger,
	}
}

// PluginInfo returns information describing the plugin.
func (d *RayDriverPlugin) PluginInfo() (*base.PluginInfoResponse, error) {
	return pluginInfo, nil
}

// ConfigSchema returns the plugin configuration schema.
func (d *RayDriverPlugin) ConfigSchema() (*hclspec.Spec, error) {
	return configSpec, nil
}

// SetConfig is called by the client to pass the configuration for the plugin.
func (d *RayDriverPlugin) SetConfig(cfg *base.Config) error {
	var config Config
	if len(cfg.PluginConfig) != 0 {
		if err := base.MsgPackDecode(cfg.PluginConfig, &config); err != nil {
			return err
		}
	}

	// Save the configuration to the plugin
	d.config = &config

	// TODO: parse and validated any configuration value if necessary.
	//
	// If your driver agent configuration requires any complex validation
	// (some dependency between attributes) or special data parsing (the
	// string "10s" into a time.Interval) you can do it here and update the
	// value in d.config.
	//
	// In the example below we check if the shell specified by the user is
	// supported by the plugin.
	// shell := d.config.Shell
	// if shell != "bash" && shell != "fish" {
	// 	return fmt.Errorf("invalid shell %s", d.config.Shell)
	// }

	// Save the Nomad agent configuration
	if cfg.AgentConfig != nil {
		d.nomadConfig = cfg.AgentConfig.Driver
	}

	// TODO: initialize any extra requirements if necessary.
	//
	// Here you can use the config values to initialize any resources that are
	// shared by all tasks that use this driver, such as a daemon process.
	client, err := d.getRayConfig(config.RayClusterEndpoint)
	if err != nil {
		return fmt.Errorf("failed to get ray client: %v", err)
	}
	d.client = client

	return nil
}

func (d *RayDriverPlugin) getRayConfig(cluster string) (rayRestInterface, error) {
	return rayRestClient{
		rayClusterEndpoint: cluster,
	}, nil
}

// TaskConfigSchema returns the HCL schema for the configuration of a task.
func (d *RayDriverPlugin) TaskConfigSchema() (*hclspec.Spec, error) {
	return taskConfigSpec, nil
}

// Capabilities returns the features supported by the driver.
func (d *RayDriverPlugin) Capabilities() (*drivers.Capabilities, error) {
	return capabilities, nil
}

// Fingerprint returns a channel that will be used to send health information
// and other driver specific node attributes.
func (d *RayDriverPlugin) Fingerprint(ctx context.Context) (<-chan *drivers.Fingerprint, error) {
	ch := make(chan *drivers.Fingerprint)
	go d.handleFingerprint(ctx, ch)
	return ch, nil
}

// handleFingerprint manages the channel and the flow of fingerprint data.
func (d *RayDriverPlugin) handleFingerprint(ctx context.Context, ch chan<- *drivers.Fingerprint) {
	defer close(ch)

	// Nomad expects the initial fingerprint to be sent immediately
	ticker := time.NewTimer(0)
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			// after the initial fingerprint we can set the proper fingerprint
			// period
			ticker.Reset(fingerprintPeriod)
			ch <- d.buildFingerprint(ctx)
		}
	}
}

// buildFingerprint returns the driver's fingerprint data
func (d *RayDriverPlugin) buildFingerprint(ctx context.Context) *drivers.Fingerprint {
	// Fingerprinting is used by the plugin to relay two important information
	// to Nomad: health state and node attributes.
	//
	// If the plugin reports to be unhealthy, or doesn't send any fingerprint
	// data in the expected interval of time, Nomad will restart it.
	//
	// Node attributes can be used to report any relevant information about
	// the node in which the plugin is running (specific library availability,
	// installed versions of a software etc.). These attributes can then be
	// used by an operator to set job constrains.
	//
	// In the example below we check if the shell specified by the user exists
	// in the node.
	var health drivers.HealthState
	var desc string
	attrs := map[string]*pstructs.Attribute{}

	if d.config.Enabled {
		if err := d.client.DescribeCluster(ctx); err != nil {
			health = drivers.HealthStateUnhealthy
			desc = err.Error()
			attrs["driver.ray"] = pstructs.NewBoolAttribute(false)
		} else {
			health = drivers.HealthStateHealthy
			desc = "Healthy"
			attrs["driver.ray"] = pstructs.NewBoolAttribute(true)
		}
	} else {
		health = drivers.HealthStateUndetected
		desc = "disabled"
	}

	return &drivers.Fingerprint{
		Attributes:        attrs,
		Health:            health,
		HealthDescription: desc,
	}
}

// StartTask returns a task handle and a driver network if necessary.
func (d *RayDriverPlugin) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task with ID %q already started", cfg.ID)
	}

	var driverConfig TaskConfig
	if err := cfg.DecodeDriverConfig(&driverConfig); err != nil {
		return nil, nil, fmt.Errorf("failed to decode driver config: %v", err)
	}

	d.logger.Info("starting task", "driver_cfg", hclog.Fmt("%+v", driverConfig))
	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = cfg
	actorId := driverConfig.ActorName + "_" + strings.ReplaceAll(cfg.AllocID, "-", "")

	// TODO: implement driver specific mechanism to start the task.
	//
	// Once the task is started you will need to store any relevant runtime
	// information in a taskHandle and TaskState. The taskHandle will be
	// stored in-memory in the plugin and will be used to interact with the
	// task.
	//
	// The TaskState will be returned to the Nomad client inside a
	// drivers.TaskHandle instance. This TaskHandle will be sent back to plugin
	// if the task ever needs to be recovered, so the TaskState should contain
	// enough information to handle that.
	//
	// In the example below we use an executor to fork a process to run our
	// greeter. The executor is then stored in the handle so we can access it
	// later and the the plugin.Client is used to generate a reattach
	// configuration that can be used to recover communication with the task.
	d.logger.Info("Opening stdout", "path", cfg.StdoutPath)
	stdout, err := fifo.OpenWriter(cfg.StdoutPath)
	if err != nil {
		d.logger.Error("failed to open stdout writer", "error", err, "path", cfg.StdoutPath)
		return nil, nil, fmt.Errorf("failed to open FIFO writer: %v", err)
	}

	fmt.Fprintf(stdout, "Starting task\n")

	taskCtx, taskCancel := context.WithCancel(context.Background())

	d.logger.Info("Submitting Job to Ray", "actor_id", actorId)
	_, err = d.client.RunTask(taskCtx, driverConfig, actorId)
	if err != nil {
		taskCancel()
		d.logger.Error("Failed to start Ray task", "error", err)
		fmt.Fprintf(stdout, "failed to start task: %v\n", err)
		return nil, nil, nstructs.NewRecoverableError(fmt.Errorf("failed to start ray task"), true)
	}
	d.logger.Info("Job submitted to Ray", "actor_id", actorId)
	fmt.Fprintf(stdout, "task started - %s\n", actorId)

	h := &taskHandle{
		ActorID:      actorId,
		ctx:          taskCtx,
		cancel:       taskCancel,
		taskConfig:   cfg,
		procState:    drivers.TaskStateRunning,
		startedAt:    time.Now().Round(time.Millisecond),
		logger:       d.logger,
		doneCh:       make(chan struct{}),
		driverConfig: driverConfig,
		stdoutLogger: stdout,
	}
	fmt.Fprintf(stdout, "task handle created - %s\n", actorId)

	driverState := TaskState{
		ActorID:    actorId,
		TaskConfig: cfg,
		StartedAt:  h.startedAt,
	}

	if err := handle.SetDriverState(&driverState); err != nil {
		taskCancel()
		d.logger.Error("failed to start task, error setting driver state", "error", err)
		return nil, nil, fmt.Errorf("failed to set driver state: %v", err)
	}
	fmt.Fprintf(stdout, "driver state set - %s\n", actorId)
	d.logger.Info("Setting task handle", "actor_id", actorId)
	d.tasks.Set(cfg.ID, h)
	go h.run()

	return handle, nil, nil
}

// RecoverTask recreates the in-memory state of a task from a TaskHandle.
func (d *RayDriverPlugin) RecoverTask(handle *drivers.TaskHandle) error {
	if handle == nil {
		return errors.New("error: handle cannot be nil")
	}
	stdout, err := fifo.OpenWriter(handle.Config.StdoutPath)
	if err != nil {
		return fmt.Errorf("failed to open writer while recovering task")
	}

	fmt.Fprintf(stdout, "recovering task - %s\n", handle.Config.ID)

	if _, ok := d.tasks.Get(handle.Config.ID); ok {
		return fmt.Errorf("no task to recover; task already exists")
	}

	var taskState TaskState
	if err := handle.GetDriverState(&taskState); err != nil {
		fmt.Fprintf(stdout, "failed to decode task state from handle: %v\n", err)
		return fmt.Errorf("failed to decode task state from handle: %v", err)
	}

	var driverConfig TaskConfig
	if err := taskState.TaskConfig.DecodeDriverConfig(&driverConfig); err != nil {
		fmt.Fprintf(stdout, "failed to decode driver config: %v\n", err)
		return fmt.Errorf("failed to decode driver config: %v", err)
	}

	// TODO: implement driver specific logic to recover a task.
	//
	// Recovering a task involves recreating and storing a taskHandle as if the
	// task was just started.
	//
	// In the example below we use the executor to re-attach to the process
	// that was created when the task first started.
	actorId := driverConfig.ActorName + "_" + strings.ReplaceAll(handle.Config.AllocID, "-", "")

	taskCtx, taskCancel := context.WithCancel(context.Background())

	_, err = d.client.RunTask(taskCtx, driverConfig, actorId)
	if err != nil {
		taskCancel()
		fmt.Fprintf(stdout, "failed to start task: %v\n", err)
		return nstructs.NewRecoverableError(fmt.Errorf("failed to start ray task"), true)
	}
	fmt.Fprintf(stdout, "task started - %s\n", actorId)

	h := &taskHandle{
		ActorID:      taskState.ActorID,
		taskConfig:   taskState.TaskConfig,
		procState:    drivers.TaskStateRunning,
		startedAt:    taskState.StartedAt,
		exitResult:   &drivers.ExitResult{},
		ctx:          taskCtx,
		cancel:       taskCancel,
		logger:       d.logger,
		doneCh:       make(chan struct{}),
		driverConfig: driverConfig,
		stdoutLogger: stdout,
	}
	fmt.Fprintf(stdout, "task handle created - %s\n", taskState.ActorID)
	d.tasks.Set(taskState.TaskConfig.ID, h)
	fmt.Fprintf(stdout, "task handle set - %s\n", taskState.TaskConfig.ID)
	go h.run()
	return nil
}

// WaitTask returns a channel used to notify Nomad when a task exits.
func (d *RayDriverPlugin) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}
	stdout := handle.stdoutLogger
	fmt.Fprintf(stdout, "inside wait task \n")
	d.logger.Info("inside wait task", "actor_id", handle.ActorID)
	ch := make(chan *drivers.ExitResult)
	go d.handleWait(ctx, handle, ch)
	return ch, nil
}

func (d *RayDriverPlugin) handleWait(ctx context.Context, handle *taskHandle, ch chan *drivers.ExitResult) {
	defer close(ch)
	var result *drivers.ExitResult

	// TODO: implement driver specific logic to notify Nomad the task has been
	// completed and what was the exit result.
	//
	// When a result is sent in the result channel Nomad will stop the task and
	// emit an event that an operator can use to get an insight on why the task
	// stopped.
	//
	// In the example below we block and wait until the executor finishes
	// running, at which point we send the exit code and signal in the result
	// channel.
	select {
	case <-ctx.Done():
		d.logger.Debug("Context cancelled in handleWait")
		return
	case <-d.ctx.Done():
		d.logger.Debug("Driver context cancelled in handleWait")
		return
	case <-handle.doneCh:
		d.logger.Debug("Task completed normally via doneCh")
		result = &drivers.ExitResult{
			ExitCode: handle.exitResult.ExitCode,
			Signal:   handle.exitResult.Signal,
			Err:      handle.exitResult.Err,
		}
	}

	select {
	case <-ctx.Done():
		return
	case <-d.ctx.Done():
		return
	case ch <- result:
	}
}

// StopTask stops a running task with the given signal and within the timeout window.
func (d *RayDriverPlugin) StopTask(taskID string, timeout time.Duration, signal string) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}
	// TODO: implement driver specific logic to stop a task.
	//
	// The StopTask function is expected to stop a running task by sending the
	// given signal to it. If the task does not stop during the given timeout,
	// the driver must forcefully kill the task.
	//
	// In the example below we let the executor handle the task shutdown
	// process for us, but you might need to customize this for your own
	// implementation.
	stdout := handle.stdoutLogger
	fmt.Fprintf(stdout, "stopping task with detach mode - %t\n", signal == drivers.DetachSignal)

	d.logger.Info("Calling handle.stopTask()", "actor_id", handle.ActorID)
	handle.stopTask()

	d.logger.Info("Calling handle.stop()", "actor_id", handle.ActorID)
	handle.stop()

	select {
	case <-handle.doneCh:
		d.logger.Info("Task stopped gracefully", "actor_id", handle.ActorID)
		fmt.Fprintf(stdout, "task stopped gracefully\n")
	case <-time.After(timeout):
		d.logger.Warn("Task did not stop within timeout", "actor_id", handle.ActorID)
		fmt.Fprintf(stdout, "task did not stop within timeout\n")
	}

	fmt.Fprintf(stdout, "task stopped - [%s]\n", handle.ActorID)

	return nil
}

// DestroyTask cleans up and removes a task that has terminated.
func (d *RayDriverPlugin) DestroyTask(taskID string, force bool) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if handle.IsRunning() && !force {
		return errors.New("cannot destroy running task")
	}

	// TODO: implement driver specific logic to destroy a complete task.
	//
	// Destroying a task includes removing any resources used by task and any
	// local references in the plugin. If force is set to true the task should
	// be destroyed even if it's currently running.
	//
	// In the example below we use the executor to force shutdown the task
	// (timeout equals 0).
	stdout := handle.stdoutLogger
	d.logger.Info("running destroy task", "actor_id", handle.ActorID)
	fmt.Fprintf(stdout, "running destroy task, with force mode - %t\n", force)

	// First stop the task and wait for cleanup
	handle.stop()

	// Wait for run() goroutine to finish cleanup
	select {
	case <-handle.doneCh:
		fmt.Fprintf(stdout, "task cleanup completed - [%s]\n", taskID)
	case <-time.After(30 * time.Second):
		fmt.Fprintf(stdout, "timeout waiting for task cleanup - [%s]\n", taskID)
	}

	// Now safe to remove from task store
	d.tasks.Delete(taskID)

	fmt.Fprintf(stdout, "task destroyed - [%s]\n", taskID)
	return nil
}

// InspectTask returns detailed status information for the referenced taskID.
func (d *RayDriverPlugin) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	return handle.TaskStatus(), nil
}

// TaskStats returns a channel which the driver should send stats to at the given interval.
func (d *RayDriverPlugin) TaskStats(ctx context.Context, taskID string, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	_, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	// This function returns a channel that Nomad will use to listen for task
	// stats (e.g., CPU and memory usage) in a given interval. It should send
	// stats until the context is canceled or the task stops running.
	//
	// In the example below we use the Stats function provided by the executor,
	// but you can build a set of functions similar to the fingerprint process.
	ch := make(chan *drivers.TaskResourceUsage)

	go func() {
		defer d.logger.Info("stopped sending ray task stats", "task_id", taskID)
		defer close(ch)
		for {
			select {
			case <-time.After(interval):

				// Nomad core does not currently have any resource based
				// support for remote drivers. Once this changes, we may be
				// able to report actual usage here.
				//
				// This is required, otherwise the driver panics.
				ch <- &structs.TaskResourceUsage{
					ResourceUsage: &drivers.ResourceUsage{
						MemoryStats: &drivers.MemoryStats{},
						CpuStats:    &drivers.CpuStats{},
					},
					Timestamp: time.Now().UTC().UnixNano(),
				}
			case <-ctx.Done():
				return
			}

		}
	}()

	return ch, nil
}

// TaskEvents returns a channel that the plugin can use to emit task related events.
func (d *RayDriverPlugin) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return d.eventer.TaskEvents(ctx)
}

// SignalTask forwards a signal to a task.
// This is an optional capability.
func (d *RayDriverPlugin) SignalTask(taskID string, signal string) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	// TODO: implement driver specific signal handling logic.
	//
	// The given signal must be forwarded to the target taskID. If this plugin
	// doesn't support receiving signals (capability SendSignals is set to
	// false) you can just return nil.
	stdout := handle.stdoutLogger
	d.logger.Info("Signal received", "signal", signal, "task_id", handle.ActorID)
	fmt.Fprintf(stdout, "%s signal received, deleting task \n", signal)

	d.tasks.Delete(taskID)
	return nil
}

// ExecTask returns the result of executing the given command inside a task.
// This is an optional capability.
func (d *RayDriverPlugin) ExecTask(taskID string, cmd []string, timeout time.Duration) (*drivers.ExecTaskResult, error) {
	// TODO: implement driver specific logic to execute commands in a task.
	return nil, errors.New("This driver does not support exec")
}
