// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package ray

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	// "os/exec"
	// "strings"
	"net/http"
	"text/template"
	"time"

	"github.com/Alfred-hq/nomad-ray-driver-plugin/templates"
)

// rayRestInterface encapsulates all the required ray rest functionality to
// successfully run tasks via this plugin.
type rayRestInterface interface {

	// DescribeCluster is used to determine the health of the plugin by
	// querying REST server for the cluster and checking its current status. A status
	// other than ACTIVE is considered unhealthy.
	DescribeCluster(ctx context.Context) error

	// RunTask is used to trigger the running of a new RAY REST task based on the
	// provided configuration. Any errors are
	// returned to the caller.
	RunTask(ctx context.Context, cfg TaskConfig, actorId string) (string, error)

	DeleteActor(ctx context.Context, actor_id string) (string, error)

	GetActorLogsCLI(ctx context.Context, actorID string) (string, error)

	GetActorStatusCLI(ctx context.Context, actorID string) (string, error)

	DeleteActorCLI(ctx context.Context, actorID string) (string, error)

	GetActorMemory(ctx context.Context, metricsEndpoint string, actorID string) (int64, error)
	// // StopTask stops the running ECS task, adding a custom message which can
	// // be viewed via the AWS console specifying it was this Nomad driver which
	// // performed the action.
	// StopTask(ctx context.Context, taskARN string) error
}

type rayRestClient struct {
	rayClusterEndpoint string
}

type ActorStatusResponse struct {
	Status      string `json:"status"`
	ActorStatus string `json:"actor_status,omitempty"`
	Error       string `json:"error,omitempty"`
}

// func getTailscaleIP() (string, error) {
// 	// Run the complete command with grep to get the IP directly
// 	cmd := exec.Command("sh", "-c", "ip -4 addr show tailscale0 | grep -oP '(?<=inet\\s)\\d+(\\.\\d+){3}'")
// 	output, err := cmd.Output()
// 	if err != nil {
// 		return "", fmt.Errorf("failed to get tailscale IP: %v", err)
// 	}

// 	// Trim any whitespace or newlines from the output
// 	ip := strings.TrimSpace(string(output))
// 	if ip == "" {
// 		return "", fmt.Errorf("no IP address found for tailscale0 interface")
// 	}

// 	return ip, nil
// }

// DescribeCluster satisfies the DescribeCluster
// interface function.
func (c rayRestClient) DescribeCluster(ctx context.Context) error {
	// Construct the full URL with the IP and port
	// ip, err := getTailscaleIP()
	// if err != nil {
	// 	return fmt.Errorf("failed to get tailscale IP: %v", err)
	// }

	// Construct the endpoint URL using the obtained IP
	// endpoint := fmt.Sprintf("http://%s:8265", ip)
	url := fmt.Sprintf("%s/api/version", c.rayClusterEndpoint)

	// Make a GET request to the REST API
	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("failed to call ray API at %s: %v", url, err)
	}
	defer resp.Body.Close()

	// Check if the HTTP status code is not OK
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ray API request to %s failed with status code: %d", url, resp.StatusCode)
	}

	// If the request is successful and the status code is 200 (OK)
	return nil
}

// generateScript generates a Python script from a given template and task configuration.
func generateScript(tmplContent string, task interface{}) (string, error) {
	tmpl, err := template.New("pythonScript").Parse(tmplContent)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}

	var script bytes.Buffer
	err = tmpl.Execute(&script, task)
	if err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return script.String(), nil
}

// submitJob submits a job to the Ray cluster and handles the HTTP request and response.
func submitJob(ctx context.Context, endpoint string, entrypoint string, jobSubmissionID string) (string, error) {
	// Build the request payload
	payload := map[string]interface{}{
		"entrypoint":  entrypoint,
		"runtime_env": map[string]interface{}{},
		"job_id":      nil,
		"metadata":    map[string]string{"job_submission_id": jobSubmissionID},
	}

	// Convert payload to JSON
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Create the HTTP request
	url := fmt.Sprintf("%s/api/jobs/", endpoint)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read and process the response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	// Check for success
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("request failed with status: %s, response: %s", resp.Status, string(body))
	}

	return string(body), nil
}

// DeleteActor sends a DELETE request to the specified URL
func (c rayRestClient) DeleteActor(ctx context.Context, actor_id string) (string, error) {
	rayServeEndpoint := "http://localhost:8000"
	url := rayServeEndpoint + "/api/kill-actor?actor_id=" + actor_id

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return "", fmt.Errorf("failed to create delete request: %w", err)
	}
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to delete actor: %w %s", err, url)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	var response ActorStatusResponse
	err = json.Unmarshal(responseBody, &response)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if response.Status != "success" {
		return "", fmt.Errorf("error from server: %s", response.Error)
	}

	return response.Status, nil
}

func (c rayRestClient) RunTask(ctx context.Context, cfg TaskConfig, actorId string) (string, error) {
	actorStatus, err := c.GetActorStatusCLI(ctx, actorId)

	if actorStatus != "ALIVE" || err != nil {
		scriptContent, err := generateScript(templates.RayActorTemplate, cfg)
		if err != nil {
			return "", fmt.Errorf("failed to generate script: %w", err)
		}

		entrypoint := fmt.Sprintf(`python3 -c """%s"""`, scriptContent)

		_, err = submitJob(ctx, cfg.RayClusterEndpoint, entrypoint, "127")
		if err != nil {
			return "", err
		}

		time.Sleep(20 * time.Second)

		scriptContent, err = generateScript(templates.RemoteRunnerTemplate, cfg)
		if err != nil {
			return "", fmt.Errorf("failed to generate runner script: %w", err)
		}

		entrypoint = fmt.Sprintf(`python3 -c """%s"""`, scriptContent)
		_, err = submitJob(ctx, cfg.RayClusterEndpoint, entrypoint, "129")
		if err != nil {
			return "", err
		}

		time.Sleep(10 * time.Second)
	}

	// Process the response if needed, assuming the actor's name is returned
	return actorId, nil
}

func runCommand(ctx context.Context, command string) (string, error) {
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(stderr.String()))
	}

	return strings.TrimSpace(stdout.String()), nil
}

// GetActorLogs sends a POST request to retrieve logs of a specific actor
func (c rayRestClient) GetActorLogsCLI(ctx context.Context, actorID string) (string, error) {
	rayAddress := c.rayClusterEndpoint
	command := fmt.Sprintf("ray list actors --address %s --filter 'state=ALIVE' | grep %s", rayAddress, actorID)
	actorDetails, err := runCommand(ctx, command)
	if err != nil {
		return "", fmt.Errorf("failed to fetch actor details: %v", err)
	}

	// Parse the actor ID from the command output
	parts := strings.Fields(actorDetails)
	if len(parts) < 4 {
		return "", fmt.Errorf("unable to parse actor ID from output: %s", actorDetails)
	}
	id := parts[1] // Extract the actor ID (assumes it's the second part)

	// Step 2: Fetch logs for the actor
	// TODO: use varibale
	logsCommand := fmt.Sprintf("ray logs actor --address localhost:6379 --id %s --tail 100", id)
	logs, err := runCommand(ctx, logsCommand)
	if err != nil {
		return "", fmt.Errorf("failed to fetch actor logs: %v", err)
	}

	// Return the logs
	return logs, nil
}

// GetActorStatus sends a POST request to the specified URL with the given actor_id
func (c rayRestClient) GetActorStatusCLI(ctx context.Context, actorID string) (string, error) {
	rayAddress := c.rayClusterEndpoint
	command := fmt.Sprintf("ray list actors --address %s --filter 'state=ALIVE' | grep %s", rayAddress, actorID)
	actorDetails, err := runCommand(ctx, command)
	if err != nil {
		return "", fmt.Errorf("failed to fetch actor details: %v", err)
	}

	// Parse the actor status from the command output
	parts := strings.Fields(actorDetails)
	if len(parts) < 4 {
		return "", fmt.Errorf("unable to parse actor status from output: %s", actorDetails)
	}
	actorStatus := parts[3] // Extract the actor status (assumes it's the fourth part)

	return actorStatus, nil
}

func (c rayRestClient) DeleteActorCLI(ctx context.Context, actorID string) (string, error) {

	// Inline Python script for killing the actor
	pythonCode := fmt.Sprintf(`
import ray
import sys

ray.init(address="ray://localhost:10001")

try:
	actor = ray.get_actor(name="%s", namespace="public91")
	ray.kill(actor)
	print("Actor deleted successfully")
	sys.exit(0)
except Exception as e:
	print(f"Failed to kill actor: {str(e)}", file=sys.stderr)
	sys.exit(1)
`, actorID)

	// Execute the Python code using the shell
	cmd := exec.CommandContext(ctx, "python3", "-c", pythonCode)
	output, err := cmd.CombinedOutput()

	// Check for errors and process the response
	if err != nil {
		// Non-zero exit code indicates failure
		return "", fmt.Errorf("failed to delete actor: %w\nOutput: %s", err, strings.TrimSpace(string(output)))
	}

	// Success
	return strings.TrimSpace(string(output)), nil
}

func (c rayRestClient) GetActorMemory(ctx context.Context, metricsEndpoint string, actorID string) (int64, error) {

	// Append ".runner" to the actorID for metric matching
	actorIDWithSuffix := fmt.Sprintf(`%s.runner`, actorID)

	resp, err := http.Get(metricsEndpoint)
	if err != nil {
		return 0, fmt.Errorf("error fetching metrics: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	var value float64

	// Use regex to match the metric line
	regexPattern := fmt.Sprintf(`ray_component_uss_mb{Component="ray::%s".*} ([0-9.]+)`, regexp.QuoteMeta(actorIDWithSuffix))
	metricRegex := regexp.MustCompile(regexPattern)

	for scanner.Scan() {
		line := scanner.Text()

		// Find matching metric line
		matches := metricRegex.FindStringSubmatch(line)
		if len(matches) == 2 {
			// Parse the matched value
			value, err = strconv.ParseFloat(matches[1], 64)
			if err != nil {
				return 0, fmt.Errorf("error parsing value: %v", err)
			}
			break
		}
	}

	if value == 0 {
		return 0, fmt.Errorf("metric not found or value is zero")
	}

	return int64(value), nil
}
