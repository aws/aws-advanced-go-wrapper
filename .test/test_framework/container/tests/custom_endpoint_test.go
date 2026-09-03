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
	"database/sql"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/.test/test_framework/container/test_utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/property_util"
	_ "github.com/aws/aws-advanced-go-wrapper/custom-endpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Custom endpoint integration tests.
//
// These exist because the plugin shipped inert for five months: it subscribed to a method name the
// plugin manager never dispatches, so it was silently dropped from the connect pipeline and applied
// no host filtering at all. Nothing caught it, because there was no integration coverage and the unit
// tests called plugin.Connect directly rather than going through the plugin manager.
//
// The assertion that matters throughout is that the driver will not select an instance outside the
// custom endpoint's member list. The initial connection is deliberately never used as evidence of
// that: Aurora's own DNS already restricts it to members, so it looks correct even against a build
// that filters nothing. Only hosts the driver chooses for itself can show the difference, which is why
// every case here drives read/write splitting or failover.

const (
	// customEndpointQueryTimeoutSeconds bounds each instance-identity query.
	customEndpointQueryTimeoutSeconds = 15

	// customEndpointMinInstances is three because the tests need a writer, a reader that is a member,
	// and a reader that is not. Below that, "excluded from the endpoint" and "absent from the cluster"
	// are indistinguishable and the tests would pass against an unfiltered build.
	customEndpointMinInstances = 3

	// customEndpointRefreshRateMs is the driver's own poll interval, set low so the tests do not have
	// to wait out the 30s default after changing an endpoint.
	customEndpointRefreshRateMs = 10000

	// monitorPickupWait is how long to wait for the driver's monitor to observe a member-list change
	// that the RDS API has already confirmed. Two poll intervals plus margin: one interval may already
	// have been in flight when the change landed.
	monitorPickupWait = 3 * customEndpointRefreshRateMs * time.Millisecond
)

type customEndpointTestSetup struct {
	env         *test_utils.TestEnvironment
	auroraUtil  *test_utils.AuroraTestUtility
	endpointId  string
	endpointUrl string
	members     []string
	writerId    string
}

// setupCustomEndpointTest creates a custom endpoint whose only member is the cluster writer, and
// registers its deletion. A single-member endpoint is the sharpest starting point: every instance in
// the cluster except one is excluded, so an unfiltered build is wrong almost immediately.
func setupCustomEndpointTest(t *testing.T) *customEndpointTestSetup {
	require.NoError(t, test_utils.BasicSetup(t.Name()))

	env, err := test_utils.GetCurrentTestEnvironment()
	require.NoError(t, err)

	// Custom endpoints are an Aurora cluster feature. RDS Multi-AZ clusters and Limitless shard groups
	// do not have them, and there is nothing to degrade to.
	if env.Info().Request.Deployment != test_utils.AURORA {
		t.Skipf("Skipping %s: custom endpoints require an Aurora deployment, got %s.",
			t.Name(), env.Info().Request.Deployment)
	}
	test_utils.SkipForTestEnvironmentFeatures(t, env.Info().Request.Features,
		test_utils.LIMITLESS_DEPLOYMENT, test_utils.PERFORMANCE)
	test_utils.SkipIfInsufficientInstances(t, env, customEndpointMinInstances)

	auroraUtil := test_utils.NewAuroraTestUtility(env.Info())
	clusterId := env.Info().RdsDbName()

	writerId, err := auroraUtil.GetClusterWriterInstanceId(clusterId)
	require.NoError(t, err)

	// Named after the test so a leaked endpoint from an aborted run is attributable, and suffixed with
	// the start time so a re-run does not collide with one that is still being deleted.
	endpointId := fmt.Sprintf("go-wrapper-ce-%d", time.Now().UnixNano()/int64(time.Millisecond))

	ctx := context.Background()
	require.NoError(t, auroraUtil.CreateCustomEndpoint(ctx, endpointId, clusterId, []string{writerId}))
	t.Cleanup(func() {
		if err := auroraUtil.DeleteCustomEndpoint(context.Background(), endpointId); err != nil {
			// Logged rather than failed: a leaked endpoint is a cleanup problem, not a test result, and
			// failing here would mask the real outcome.
			slog.Error("Could not delete the test custom endpoint. It may need removing by hand.",
				"endpoint", endpointId, "error", err)
		}
		test_utils.BasicCleanup(t.Name())
	})

	endpoint, err := auroraUtil.WaitUntilCustomEndpointAvailable(
		ctx, endpointId, test_utils.CustomEndpointAvailableTimeout)
	require.NoError(t, err)
	require.NotNil(t, endpoint.Endpoint, "the created custom endpoint has no URL")

	slog.Info("Custom endpoint ready.",
		"endpoint", endpointId, "url", *endpoint.Endpoint, "members", endpoint.StaticMembers)

	return &customEndpointTestSetup{
		env:         env,
		auroraUtil:  auroraUtil,
		endpointId:  endpointId,
		endpointUrl: *endpoint.Endpoint,
		members:     endpoint.StaticMembers,
		writerId:    writerId,
	}
}

// openDb connects through the custom endpoint URL rather than the cluster endpoint, which is what
// makes the plugin engage at all: it returns early unless the host is a cluster-custom- DNS name.
func (s *customEndpointTestSetup) openDb(t *testing.T, plugins string, extra map[string]string) *sql.DB {
	t.Helper()
	props := map[string]string{
		property_util.PLUGINS.Name:                              plugins,
		property_util.HOST.Name:                                 s.endpointUrl,
		property_util.PORT.Name:                                 strconv.Itoa(s.env.Info().DatabaseInfo.ClusterEndpointPort),
		property_util.CUSTOM_ENDPOINT_INFO_REFRESH_RATE_MS.Name: strconv.Itoa(customEndpointRefreshRateMs),
	}
	for key, value := range extra {
		props[key] = value
	}

	db, err := test_utils.OpenDb(s.env.Info().Request.Engine, test_utils.GetDsn(s.env, props))
	require.NoError(t, err)
	// One connection, so a read-only switch is observable rather than being satisfied by a different
	// pooled connection that happens to already be on a reader.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.Ping())
	return db
}

func (s *customEndpointTestSetup) instanceId(rowQuerier test_utils.RowQuerier, ctx context.Context) (string, error) {
	return test_utils.ExecuteInstanceQueryContextWithTimeout(
		s.env.Info().Request.Engine, s.env.Info().Request.Deployment,
		rowQuerier, customEndpointQueryTimeoutSeconds, ctx)
}

// readerNotInEndpoint returns a cluster reader that the custom endpoint excludes. This is the instance
// an unfiltered build can reach and a correct one cannot.
func (s *customEndpointTestSetup) readerNotInEndpoint(t *testing.T) string {
	t.Helper()
	for _, instance := range s.env.Info().DatabaseInfo.Instances {
		id := instance.InstanceId()
		if id != s.writerId && !slices.Contains(s.members, id) {
			return id
		}
	}
	t.Fatalf("no cluster reader outside the custom endpoint's members %v; the test cannot distinguish "+
		"filtered from unfiltered host selection", s.members)
	return ""
}

// TestCustomEndpointRestrictsReaderSelection is the direct regression test for the plugin having been
// inert, and the cheapest of these to run.
//
// The endpoint's only member is the writer, so there is no reader the driver may select. Asking for a
// read-only connection must therefore either stay on the writer or fail; what it must not do is reach
// one of the readers the endpoint excludes. Against the inert plugin, GetHosts returned the whole
// cluster topology and this landed on an excluded reader.
func TestCustomEndpointRestrictsReaderSelection(t *testing.T) {
	setup := setupCustomEndpointTest(t)
	excludedReader := setup.readerNotInEndpoint(t)

	db := setup.openDb(t, "customEndpoint,readWriteSplitting", nil)

	initialId, err := setup.instanceId(db, writeCtx)
	require.NoError(t, err)
	assert.Contains(t, setup.members, initialId,
		"the initial connection is outside the custom endpoint's member list")

	readOnlyId, err := setup.instanceId(db, readOnlyCtx)
	if err != nil {
		// Acceptable: with no reader available the read/write splitting plugin may report that rather
		// than silently serving reads from the writer.
		slog.Info("Read-only switch was refused, which is valid for a writer-only endpoint.", "error", err)
		return
	}

	assert.NotEqual(t, excludedReader, readOnlyId,
		"the driver selected a reader that the custom endpoint excludes, so host filtering is not being applied")
	assert.Contains(t, setup.members, readOnlyId,
		"the read-only connection is outside the custom endpoint's member list")
}

// TestCustomEndpointFollowsMemberChanges covers the monitor's refresh path: a member added at the AWS
// API level has to become selectable, and a member removed has to stop being selectable.
//
// This is the case most likely to regress, because it depends on the monitor continuing to poll and to
// republish permissions over the lifetime of a connection rather than only at connect time.
func TestCustomEndpointFollowsMemberChanges(t *testing.T) {
	setup := setupCustomEndpointTest(t)
	newMember := setup.readerNotInEndpoint(t)
	ctx := context.Background()

	// A monitor expiration shorter than the time an endpoint modification takes, so the idle monitor is
	// disposed and recreated mid-test. That exercises the handover path, where a superseded monitor
	// could delete the cache entries its replacement had just published.
	db := setup.openDb(t, "customEndpoint,readWriteSplitting", map[string]string{
		property_util.CUSTOM_ENDPOINT_MONITOR_IDLE_EXPIRATION_MS.Name: "30000",
	})

	initialId, err := setup.instanceId(db, writeCtx)
	require.NoError(t, err)
	require.Contains(t, setup.members, initialId)

	// Add the excluded reader, then confirm at the API level before waiting on the driver. Separating
	// the two makes a failure attributable: if the API never converged, the driver is not at fault.
	require.NoError(t, setup.auroraUtil.SetCustomEndpointStaticMembers(
		ctx, setup.endpointId, []string{setup.writerId, newMember}))

	t.Cleanup(func() {
		// Restore the single-member list so a re-run starts from the documented state even if the test
		// fails part way through.
		if err := setup.auroraUtil.SetCustomEndpointStaticMembers(
			context.Background(), setup.endpointId, []string{setup.writerId}); err != nil {
			slog.Error("Could not restore the custom endpoint's member list.", "error", err)
		}
	})

	require.NoError(t, setup.auroraUtil.WaitUntilCustomEndpointHasMembers(
		ctx, setup.endpointId, []string{setup.writerId, newMember}, test_utils.CustomEndpointMembersTimeout))
	time.Sleep(monitorPickupWait)

	// The added reader is now a member, so a read-only switch must be able to reach it. A failover error
	// here is tolerated: the driver may notice the topology change and reconnect, which still leaves the
	// connection on a valid member.
	readerId, err := setup.instanceId(db, readOnlyCtx)
	if err != nil && !error_util.IsType(err, error_util.FailoverSuccessErrorType) {
		require.NoError(t, err, "could not switch to a reader after it was added to the custom endpoint")
	}
	if err != nil {
		readerId, err = setup.instanceId(db, readOnlyCtx)
		require.NoError(t, err)
	}
	assert.Equal(t, newMember, readerId,
		"the newly added member is the only reader in the endpoint, so the read-only connection should be on it")

	// Remove it again. The driver must stop selecting it.
	require.NoError(t, setup.auroraUtil.SetCustomEndpointStaticMembers(
		ctx, setup.endpointId, []string{setup.writerId}))
	require.NoError(t, setup.auroraUtil.WaitUntilCustomEndpointHasMembers(
		ctx, setup.endpointId, []string{setup.writerId}, test_utils.CustomEndpointMembersTimeout))
	time.Sleep(monitorPickupWait)

	afterRemovalId, err := setup.instanceId(db, readOnlyCtx)
	if err != nil {
		slog.Info("Read-only switch was refused after the reader was removed, which is valid.", "error", err)
		return
	}
	assert.NotEqual(t, newMember, afterRemovalId,
		"the driver is still selecting an instance that was removed from the custom endpoint")
}

// TestCustomEndpointFailoverStaysWithinMembers checks that failover honours the member list. Failover
// picks its own target, so it is a second, independent path into host selection.
func TestCustomEndpointFailoverStaysWithinMembers(t *testing.T) {
	setup := setupCustomEndpointTest(t)
	ctx := context.Background()

	// Both cluster readers are members here, so failover has somewhere valid to go. A writer-only
	// endpoint would leave failover no target and would test error handling instead of filtering.
	members := []string{setup.writerId}
	for _, instance := range setup.env.Info().DatabaseInfo.Instances {
		if instance.InstanceId() != setup.writerId {
			members = append(members, instance.InstanceId())
			break
		}
	}
	require.Len(t, members, 2, "need a reader to fail over to")

	require.NoError(t, setup.auroraUtil.SetCustomEndpointStaticMembers(ctx, setup.endpointId, members))
	require.NoError(t, setup.auroraUtil.WaitUntilCustomEndpointHasMembers(
		ctx, setup.endpointId, members, test_utils.CustomEndpointMembersTimeout))

	db := setup.openDb(t, "customEndpoint,readWriteSplitting,failover", map[string]string{
		property_util.FAILOVER_MODE.Name: "reader-or-writer",
	})

	initialId, err := setup.instanceId(db, writeCtx)
	require.NoError(t, err)
	require.Contains(t, members, initialId)

	// TriggerFailover rather than FailoverClusterAndWaitTillWriterChanged, matching every other failover
	// test here: it resolves the cluster from the environment and substitutes a proxy-based failure for
	// deployments where the failover API is not available.
	require.NoError(t, setup.auroraUtil.TriggerFailover(setup.writerId, "", ""))

	// The first query after failover reports it rather than returning a row.
	_, err = setup.instanceId(db, writeCtx)
	require.Error(t, err, "expected the failover to surface on the next query")
	assert.True(t, error_util.IsType(err, error_util.FailoverSuccessErrorType),
		"expected a failover error, got %v", err)

	afterFailoverId, err := setup.instanceId(db, writeCtx)
	require.NoError(t, err)
	assert.Contains(t, members, afterFailoverId,
		"failover selected an instance outside the custom endpoint's member list")
}
