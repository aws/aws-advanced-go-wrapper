/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package test

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snapshot copies the captured records under the lock. The monitor logs from its own goroutine, so
// reading the slice directly races with it.
func (h *TestHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

// captureLogs installs a capturing handler as the default logger for the duration of a test.
func captureLogs(t *testing.T) *TestHandler {
	t.Helper()
	previous := slog.Default()
	handler := &TestHandler{}
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return handler
}

// publishedEndpointOutput is a DescribeDBClusterEndpoints response the monitor can publish from.
func publishedEndpointOutput(endpointId string) *rds.DescribeDBClusterEndpointsOutput {
	return &rds.DescribeDBClusterEndpointsOutput{
		DBClusterEndpoints: []types.DBClusterEndpoint{{
			DBClusterEndpointIdentifier: &endpointId,
			DBClusterIdentifier:         &endpointId,
			Endpoint:                    &endpointId,
			CustomEndpointType:          aws.String("READER"),
			StaticMembers:               []string{"instance-1"},
		}},
	}
}

// blockingRdsApi signals that a fetch has begun and then blocks until the caller's context is
// cancelled, which is what Stop does. It lets a test observe how the monitor reports its own
// cancellation, as distinct from a real service failure.
type blockingRdsApi struct {
	started   chan struct{}
	startOnce sync.Once
}

func (b *blockingRdsApi) DescribeDBClusterEndpoints(
	ctx context.Context,
	_ *rds.DescribeDBClusterEndpointsInput,
	_ ...func(*rds.Options),
) (*rds.DescribeDBClusterEndpointsOutput, error) {
	b.startOnce.Do(func() { close(b.started) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestCustomEndpointMonitor_StopDoesNotLogFetchFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	api := &blockingRdsApi{started: make(chan struct{})}

	monitor := newTestMonitor(t, mockContainer, "stop-quietly", time.Second, time.Minute, 2, mockCounter, api)

	monitor.Start()
	select {
	case <-api.started:
	case <-time.After(3 * time.Second):
		t.Fatal("the monitor never issued a fetch")
	}

	handler := captureLogs(t)
	monitor.Stop()

	for _, record := range handler.snapshot() {
		assert.NotEqual(t, slog.LevelError, record.Level,
			"stopping the monitor logged an error indistinguishable from a real monitoring failure: %s",
			record.Message)
	}
}

// TestCustomEndpointMonitor_ReplacementMonitorKeepsOwnedKey pins the ownership semantics the
// handover guard depends on. MonitorManager evicts a monitor from its cache and releases its
// initialization lock before calling Stop, so an application goroutine can create and start a
// replacement in that gap; the cache keys are URLs with no instance identity, so an outgoing
// monitor's cleanup must not touch entries its successor published.
//
// Note what this does and does not prove. It establishes that a superseded monitor leaves the
// successor's entries alone, which is the invariant. It cannot exercise the interleaving the fix
// addresses - a replacement claiming the key between the outgoing instance's ownership check and its
// removal - because that window is inside one unexported function and is not reachable from outside
// the package. That half rests on RemoveIf holding the write lock across both steps.
func TestCustomEndpointMonitor_ReplacementMonitorKeepsOwnedKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockContainer, _, mockCounter, _ := createCustomEndpointMonitorMocks(t, ctrl)
	mockCounter.EXPECT().Inc(gomock.Any()).AnyTimes()

	endpointId := "handover"
	outgoing := newTestMonitor(t, mockContainer, endpointId, 20*time.Millisecond, time.Minute, 2, mockCounter, &stubRdsApi{out: publishedEndpointOutput(endpointId)})

	outgoing.Start()
	require.Eventually(t, outgoing.HasCustomEndpointInfo, 3*time.Second, 10*time.Millisecond,
		"the outgoing monitor never published endpoint info")

	// A long refresh rate, so that once the replacement has published it sits in a sleep. Otherwise a
	// republish moments later would mask the outgoing monitor having deleted the entry.
	replacement := newTestMonitor(t, mockContainer, endpointId, 30*time.Second, time.Minute, 2, mockCounter, &stubRdsApi{out: publishedEndpointOutput(endpointId)})
	replacement.Start()
	require.Eventually(t, replacement.HasCustomEndpointInfo, 3*time.Second, 10*time.Millisecond,
		"the replacement monitor never published endpoint info")

	outgoing.Stop()

	assert.True(t, replacement.HasCustomEndpointInfo(),
		"the superseded monitor deleted the replacement's endpoint info, so HasCustomEndpointInfo "+
			"reports false and every connect and network-bound method stalls until the replacement's "+
			"next poll")

	replacement.Stop()
	assert.False(t, replacement.HasCustomEndpointInfo(),
		"the owning monitor did not release its endpoint info on stop")
}
