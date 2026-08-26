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

package custom_endpoint

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"

	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/driver_infrastructure"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils"
	"github.com/aws/aws-advanced-go-wrapper/awssql/v2/utils/telemetry"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

// CustomEndpointMonitorType is the type descriptor for custom endpoint monitors.
// Used with MonitorService to manage CustomEndpointMonitor instances.
var CustomEndpointMonitorType = &driver_infrastructure.MonitorType{Name: "CustomEndpointMonitor"}

type CustomEndpointMonitor interface {
	driver_infrastructure.Monitor
	HasCustomEndpointInfo() bool
	RequestCustomEndpointInfoUpdate()
}

var customEndpointInfoCache *utils.CacheMap[*CustomEndpointInfo] = utils.NewCache[*CustomEndpointInfo]()

// keyOwners records which monitor instance owns each endpoint key. A monitor and its replacement can run
// concurrently, because MonitorManager evicts and unlocks before calling Stop, and the keys carry no
// instance identity. Ownership stops the outgoing instance publishing over, or deleting, its successor's
// entries.
var keyOwners = utils.NewRWMap[string, *CustomEndpointMonitorImpl]()

const CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO = time.Minute * 5

// UNAUTHORIZED_SLEEP is how long the monitor pauses after the RDS API rejects its credentials.
// Retrying a permissions failure at the normal cadence only burns API quota, so back right off.
const UNAUTHORIZED_SLEEP = time.Minute * 5

// RDS_FETCH_TIMEOUT bounds a single DescribeDBClusterEndpoints call. Kept well above the AWS SDK's own
// retry budget: a deadline expiring mid-retry replaces the service error with an opaque cancellation.
// Prompt shutdown comes from the monitor context, not from this deadline.
const RDS_FETCH_TIMEOUT = time.Second * 90

// FALLBACK_REFRESH_RATE is the floor applied to a non-positive refresh rate. Not the property default,
// which lives in property_util and is not reachable from here.
const FALLBACK_REFRESH_RATE = time.Second * 30

type CustomEndpointMonitorImpl struct {
	servicesContainer      driver_infrastructure.ServicesContainer
	customEndpointHostInfo *host_info_util.HostInfo
	endpointIdentifier     string
	region                 region_util.Region
	infoChangedCounter     telemetry.TelemetryCounter
	rdsClient              rds.DescribeDBClusterEndpointsAPIClient

	// monitorCtx parents every RDS call, so Stop cancels whatever is in flight. A parent context rather
	// than a stored per-call CancelFunc, because cancellation cannot then be lost to a race between
	// deriving the context and publishing its cancel.
	monitorCtx    context.Context
	cancelMonitor context.CancelFunc

	// Monitor interface fields
	stop                      atomic.Bool
	state                     atomic.Value // driver_infrastructure.MonitorState
	lastActivityTimestampNano atomic.Int64
	wg                        sync.WaitGroup

	// Refresh control
	refreshRequired    atomic.Bool
	hasConnectionIssue atomic.Bool
	refreshMu          sync.Mutex
	refreshCond        *sync.Cond

	// Refresh pacing, owned by the monitor goroutine, so it needs no synchronisation. currentRefreshRate
	// moves between minRefreshRate and maxRefreshRate as the RDS API throttles and recovers.
	currentRefreshRate time.Duration
	minRefreshRate     time.Duration
	maxRefreshRate     time.Duration
	backoffFactor      int

	// enforceRoleFiltering carries customEndpointEnforceRoleFiltering. While false, a READER-type
	// exclusion-list endpoint publishes no role requirement, which is the pre-1.1.0 behaviour.
	enforceRoleFiltering bool

	// Log-transition tracking, monitor-goroutine owned, so a persistent failure is reported once rather
	// than every refresh interval.
	fetchFailing      bool
	lastEndpointCount int
}

func NewCustomEndpointMonitorImpl(
	servicesContainer driver_infrastructure.ServicesContainer,
	customEndpointHostInfo *host_info_util.HostInfo,
	endpointIdentifier string,
	region region_util.Region,
	refreshRate time.Duration,
	maxRefreshRate time.Duration,
	backoffFactor int,
	enforceRoleFiltering bool,
	infoChangedCounter telemetry.TelemetryCounter,
	rdsClient rds.DescribeDBClusterEndpointsAPIClient,
) *CustomEndpointMonitorImpl {
	// A non-positive rate makes every sleep a no-op, turning the loop into an unthrottled stream of RDS
	// calls.
	if refreshRate <= 0 {
		slog.Warn(error_util.GetMessage("CustomEndpointMonitorImpl.invalidRefreshRate",
			refreshRate, FALLBACK_REFRESH_RATE))
		refreshRate = FALLBACK_REFRESH_RATE
	}
	// A factor below 1 would zero or invert the interval, so the loop would call hardest while being
	// throttled. 1 disables backoff safely.
	if backoffFactor < 1 {
		slog.Warn(error_util.GetMessage("CustomEndpointMonitorImpl.invalidBackoffFactor", backoffFactor))
		backoffFactor = 1
	}
	if maxRefreshRate < refreshRate {
		slog.Warn(error_util.GetMessage("CustomEndpointMonitorImpl.maxRefreshRateBelowRefreshRate",
			maxRefreshRate, refreshRate))
		maxRefreshRate = refreshRate
	}

	monitor := &CustomEndpointMonitorImpl{
		// -1 so the first unexpected count always logs; a real count is never negative.
		lastEndpointCount:      -1,
		servicesContainer:      servicesContainer,
		customEndpointHostInfo: customEndpointHostInfo,
		endpointIdentifier:     endpointIdentifier,
		region:                 region,
		currentRefreshRate:     refreshRate,
		minRefreshRate:         refreshRate,
		maxRefreshRate:         maxRefreshRate,
		backoffFactor:          backoffFactor,
		enforceRoleFiltering:   enforceRoleFiltering,
		infoChangedCounter:     infoChangedCounter,
		rdsClient:              rdsClient,
	}
	monitor.refreshCond = sync.NewCond(&monitor.refreshMu)
	monitor.monitorCtx, monitor.cancelMonitor = context.WithCancel(context.Background())

	return monitor
}

// speedUpRefreshRate moves the interval back towards the configured rate after a good call.
func (monitor *CustomEndpointMonitorImpl) speedUpRefreshRate() {
	if monitor.currentRefreshRate <= monitor.minRefreshRate {
		return
	}
	previous := monitor.currentRefreshRate
	monitor.currentRefreshRate /= time.Duration(monitor.backoffFactor)
	if monitor.currentRefreshRate < monitor.minRefreshRate {
		monitor.currentRefreshRate = monitor.minRefreshRate
	}
	if monitor.currentRefreshRate != previous {
		slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.refreshRateChanged",
			monitor.customEndpointHostInfo.GetUrl(), previous, monitor.currentRefreshRate))
	}
}

// slowDownRefreshRate widens the interval after the RDS API throttles us.
//
// The clamp is applied before multiplying, not after. Multiplying first can overflow time.Duration
// negative, which a post-clamp would not catch and speedUpRefreshRate could never recover from,
// leaving a non-positive interval that both sleep helpers return from immediately.
func (monitor *CustomEndpointMonitorImpl) slowDownRefreshRate() {
	if monitor.currentRefreshRate >= monitor.maxRefreshRate {
		return
	}
	previous := monitor.currentRefreshRate
	if monitor.currentRefreshRate >= monitor.maxRefreshRate/time.Duration(monitor.backoffFactor) {
		monitor.currentRefreshRate = monitor.maxRefreshRate
	} else {
		monitor.currentRefreshRate *= time.Duration(monitor.backoffFactor)
	}
	// A factor of 1 leaves the interval unchanged, so there is nothing to report.
	if monitor.currentRefreshRate == previous {
		return
	}
	// Info, not Debug: a widened interval is why endpoint information goes stale, and the number of
	// steps to the ceiling is small enough that this cannot become noisy.
	slog.Info(error_util.GetMessage("CustomEndpointMonitorImpl.throttledBackingOff",
		monitor.customEndpointHostInfo.GetUrl(), previous, monitor.currentRefreshRate))
}

// fetchFailure is how a failed DescribeDBClusterEndpoints call is treated.
type fetchFailure int

const (
	// fetchThrottled - the RDS API asked us to slow down.
	fetchThrottled fetchFailure = iota
	// fetchUnauthorized - the credentials cannot describe this endpoint.
	fetchUnauthorized
	// fetchHTTPError - the service answered with some other error status.
	fetchHTTPError
	// fetchTimedOut - our own deadline expired. Distinct from fetchNoResponse: treating it as a
	// connectivity failure would suppress forced refreshes and delay recovery.
	fetchTimedOut
	// fetchNoResponse - no HTTP exchange happened at all: DNS, credentials, connectivity.
	fetchNoResponse
)

// throttleCheck is stateless; build it once rather than per failed call.
var throttleCheck = retry.IsErrorThrottles(retry.DefaultThrottles)

func classifyFetchError(err error) fetchFailure {
	if throttleCheck.IsErrorThrottle(err) == aws.TrueTernary {
		return fetchThrottled
	}
	var responseError *awshttp.ResponseError
	if errors.As(err, &responseError) {
		switch responseError.HTTPStatusCode() {
		case http.StatusUnauthorized, http.StatusForbidden:
			return fetchUnauthorized
		default:
			return fetchHTTPError
		}
	}
	// After the HTTP cases: a deadline expiring mid-retry replaces the underlying error, and must not be
	// read as a connectivity failure.
	if errors.Is(err, context.DeadlineExceeded) {
		return fetchTimedOut
	}
	return fetchNoResponse
}

// handleFetchError reports the failure and then sleeps without letting a connection-driven refresh
// request cut the sleep short. Returning early on an error path while refreshRequired is still set is
// exactly what turned this loop into an unbounded stream of RDS calls, so every branch here sleeps
// uninterruptibly.
func (monitor *CustomEndpointMonitorImpl) handleFetchError(err error) {
	// Stop cancels the in-flight call, so on shutdown the error is our own cancellation rather than a
	// monitoring failure. Reporting it would put an ERROR indistinguishable from a real permissions or
	// connectivity failure into the log on every monitor recreate, not just at driver shutdown.
	if monitor.stop.Load() {
		slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.fetchCancelledOnStop",
			monitor.customEndpointHostInfo.GetUrl()))
		return
	}
	monitor.fetchFailing = true
	// A pending refresh request has been serviced by this attempt, failed or not. Leaving it set
	// makes the interruptible sleep a no-op.
	monitor.refreshRequired.Store(false)

	failure := classifyFetchError(err)
	if failure == fetchUnauthorized {
		// No permission to describe the endpoint. This will not fix itself quickly, and retrying at the
		// normal cadence only burns API quota. Logged at Error rather than Warn because it is the one
		// failure here that is both actionable and total: every connection and query to this endpoint
		// fails until the permission is granted.
		slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.notAuthorized",
			monitor.endpointIdentifier, UNAUTHORIZED_SLEEP))
		monitor.sleepIgnoringRefreshRequests(UNAUTHORIZED_SLEEP)
		return
	}

	slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.error",
		monitor.customEndpointHostInfo.GetUrl(), err))

	// A failure with no HTTP exchange at all - DNS, credentials, connectivity - is recorded so the
	// application stops asking for forced refreshes while it persists. A timeout is deliberately not
	// treated this way: our own deadline expiring says nothing about reachability, and suppressing
	// forced refreshes on it would delay recovery.
	if failure == fetchNoResponse {
		monitor.hasConnectionIssue.Store(true)
	}

	// A deadline expiring mid-retry is most often the SDK absorbing throttling on our behalf, so it
	// widens the interval too.
	if failure == fetchThrottled || failure == fetchTimedOut {
		monitor.slowDownRefreshRate()
	}

	monitor.sleepIgnoringRefreshRequests(monitor.currentRefreshRate)
}

// markAlive records that the monitor is still running. Called from the sleep helpers, not only per
// iteration, so the monitor service does not mistake the five-minute authorization pause for a wedged
// monitor and recreate it.
func (monitor *CustomEndpointMonitorImpl) markAlive() {
	monitor.lastActivityTimestampNano.Store(time.Now().UnixNano())
}

// Start implements Monitor interface - starts the monitoring goroutine.
func (monitor *CustomEndpointMonitorImpl) Start() {
	monitor.state.Store(driver_infrastructure.MonitorStateRunning)
	monitor.markAlive()
	// Claim the key before the goroutine runs, so a monitor created as this one's replacement
	// takes ownership immediately and the outgoing instance's cleanup becomes a no-op.
	keyOwners.Put(monitor.getCustomEndpointInfoCacheKey(), monitor)
	monitor.wg.Add(1)
	go monitor.Monitor()
}

// Stop implements Monitor interface - stops the monitor and waits for cleanup.
func (monitor *CustomEndpointMonitorImpl) Stop() {
	monitor.stop.Store(true)
	// Cancel any in-flight RDS call so Stop does not wait out its deadline. Stop runs on the
	// single shared monitor-cleanup goroutine, so blocking here stalls expiry and health checks
	// for every monitor type in the process, and serialises driver shutdown.
	monitor.cancelMonitor()
	// Wake up any sleeping goroutine
	monitor.refreshMu.Lock()
	monitor.refreshCond.Broadcast()
	monitor.refreshMu.Unlock()
	monitor.wg.Wait()
	monitor.Close()
	monitor.state.Store(driver_infrastructure.MonitorStateStopped)
}

// Monitor implements Monitor interface - the main monitoring loop.
func (monitor *CustomEndpointMonitorImpl) Monitor() {
	defer func() {
		// This runs on a background goroutine, so an unrecovered panic here would take down the
		// whole host application rather than just stopping this monitor.
		if r := recover(); r != nil {
			slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.panic",
				monitor.customEndpointHostInfo.GetUrl(), r))
			monitor.state.Store(driver_infrastructure.MonitorStateError)
		}
		slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.stoppedMonitor", monitor.customEndpointHostInfo.GetUrl()))
		monitor.releaseOwnedKey()
		monitor.wg.Done()
	}()

	slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.startingMonitor", monitor.customEndpointHostInfo.GetUrl()))

	for !monitor.stop.Load() {
		start := time.Now()
		monitor.lastActivityTimestampNano.Store(time.Now().UnixNano())

		// RDS SDK call
		command := &rds.DescribeDBClusterEndpointsInput{
			DBClusterEndpointIdentifier: &monitor.endpointIdentifier,
			Filters: []types.Filter{
				{
					Name:   aws.String("db-cluster-endpoint-type"),
					Values: []string{"custom"},
				},
			},
		}
		ctx, cancel := context.WithTimeout(monitor.monitorCtx, RDS_FETCH_TIMEOUT)
		resp, err := monitor.rdsClient.DescribeDBClusterEndpoints(ctx, command)
		cancel()

		// Error checking
		if err != nil {
			monitor.handleFetchError(err)
			continue
		}
		// The call itself succeeded, so connectivity is fine regardless of what came back. Clearing
		// this after the response-shape checks below let a cluster returning an unexpected number of
		// endpoints latch the flag true and suppress forced refreshes indefinitely.
		monitor.hasConnectionIssue.Store(false)

		if resp == nil || resp.DBClusterEndpoints == nil {
			slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.nilResponse",
				monitor.customEndpointHostInfo.GetUrl()))
			monitor.refreshRequired.Store(false)
			monitor.sleepIgnoringRefreshRequests(monitor.currentRefreshRate)
			continue
		} else if len(resp.DBClusterEndpoints) != 1 {
			var endpointsString string
			for i, endpoint := range resp.DBClusterEndpoints {
				if endpoint.Endpoint != nil && *endpoint.Endpoint != "" {
					if i > 0 {
						endpointsString = endpointsString + ","
					}
					endpointsString = endpointsString + *endpoint.Endpoint
				}
			}
			// Warned on transition only. This condition is usually a persistent misconfiguration
			// (wrong identifier, deleted endpoint), and warning every refresh interval buries the
			// rest of the log without adding information.
			count := len(resp.DBClusterEndpoints)
			message := error_util.GetMessage("CustomEndpointMonitorImpl.unexpectedNumberOfEndpoints",
				monitor.endpointIdentifier, monitor.region, count, endpointsString)
			if count == monitor.lastEndpointCount {
				slog.Debug(message)
			} else {
				slog.Warn(message)
				monitor.lastEndpointCount = count
			}
			monitor.fetchFailing = true
			monitor.refreshRequired.Store(false)
			monitor.sleepIgnoringRefreshRequests(monitor.currentRefreshRate)
			continue
		}

		monitor.speedUpRefreshRate()
		monitor.noteFetchSucceeded()

		endpointInfo, err := NewCustomEndpointInfo(resp.DBClusterEndpoints[0])
		if err != nil {
			slog.Error(error_util.GetMessage("CustomEndpointMonitorImpl.unusableEndpointInfo",
				monitor.customEndpointHostInfo.GetUrl(), err))
			monitor.sleepIgnoringRefreshRequests(monitor.currentRefreshRate)
			continue
		}

		cachedEndpointInfo, ok := customEndpointInfoCache.Get(monitor.getCustomEndpointInfoCacheKey())
		changed := !ok || !endpointInfo.Equals(cachedEndpointInfo)
		if changed {
			slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.detectedChangeInCustomEndpointInfo",
				monitor.customEndpointHostInfo.GetUrl(), endpointInfo))
		}

		var allowedAndBlockedHosts *driver_infrastructure.AllowedAndBlockedHosts
		requiredRole := host_info_util.UNKNOWN
		if monitor.enforceRoleFiltering {
			requiredRole = endpointInfo.GetRequiredRole()
		}
		if STATIC_LIST == endpointInfo.memberListType {
			allowedAndBlockedHosts = driver_infrastructure.NewAllowedAndBlockedHostsWithRole(
				endpointInfo.GetStaticMembers(), nil, requiredRole)
		} else {
			allowedAndBlockedHosts = driver_infrastructure.NewAllowedAndBlockedHostsWithRole(
				nil, endpointInfo.GetExcludedMembers(), requiredRole)
		}

		// Gated on still owning the key, inseparably from the writes, so a response that started before
		// this monitor was superseded cannot overwrite its replacement's newer one.
		//
		// Republished every successful iteration, not only on change: both entries expire after 5 minutes,
		// so a change-only write let them lapse together and left host selection unfiltered.
		published := keyOwners.DoIf(monitor.getCustomEndpointInfoCacheKey(), monitor.isSelf, func() {
			driver_infrastructure.AllowedAndBlockedHostsStorageType.Set(
				monitor.servicesContainer.GetStorageService(), monitor.customEndpointHostInfo.GetUrl(), allowedAndBlockedHosts)
			customEndpointInfoCache.Put(monitor.getCustomEndpointInfoCacheKey(), endpointInfo, CUSTOM_ENDPOINT_INFO_EXPIRATION_NANO)
		})
		if !published {
			// Superseded mid-fetch. Say so once and let the loop exit on the stop flag rather than
			// carrying on as though this data mattered.
			slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.supersededBeforePublish",
				monitor.customEndpointHostInfo.GetUrl()))
			return
		}

		// After publishing, so a waiter cannot see "no refresh pending" before the data is in the cache.
		monitor.refreshRequired.Store(false)

		if changed {
			monitor.infoChangedCounter.Inc(monitor.servicesContainer.GetPluginService().GetTelemetryContext())
		}

		elapsedTime := time.Since(start)
		sleepDuration := monitor.currentRefreshRate - elapsedTime
		if sleepDuration < 0 {
			sleepDuration = 0
		}
		monitor.sleep(sleepDuration)
	}
}

// sleep waits for the specified duration, but can be interrupted by refreshRequired or stop.
func (monitor *CustomEndpointMonitorImpl) sleep(duration time.Duration) {
	if duration <= 0 {
		return
	}

	endTime := time.Now().Add(duration)
	waitDuration := min(500*time.Millisecond, duration)

	monitor.refreshMu.Lock()
	defer monitor.refreshMu.Unlock()

	for !monitor.refreshRequired.Load() && time.Now().Before(endTime) && !monitor.stop.Load() {
		monitor.markAlive()
		// Use a timer to implement timeout on the condition wait
		timer := time.AfterFunc(waitDuration, func() {
			monitor.refreshMu.Lock()
			monitor.refreshCond.Broadcast()
			monitor.refreshMu.Unlock()
		})
		monitor.refreshCond.Wait()
		timer.Stop()
	}
}

// noteFetchSucceeded reports a return to health, so an operator can confirm from the log that
// monitoring recovered rather than inferring it from errors stopping. Logged at Info because the
// failure it clears is logged at Error and leaves every connection to this endpoint failing.
func (monitor *CustomEndpointMonitorImpl) noteFetchSucceeded() {
	monitor.lastEndpointCount = -1
	if monitor.fetchFailing {
		monitor.fetchFailing = false
		slog.Info(error_util.GetMessage("CustomEndpointMonitorImpl.recovered",
			monitor.customEndpointHostInfo.GetUrl()))
	}
}

// sleepIgnoringRefreshRequests waits for the given duration without letting a connection-driven
// refresh request wake it early, which is what the interruptible sleep permits. Used on the
// error and backoff paths: if a refresh request could shorten these, the monitor would keep
// issuing RDS calls as fast as connections arrive and never actually back off. It still returns
// promptly once the monitor is stopped.
func (monitor *CustomEndpointMonitorImpl) sleepIgnoringRefreshRequests(duration time.Duration) {
	endTime := time.Now().Add(duration)
	for !monitor.stop.Load() {
		remaining := time.Until(endTime)
		if remaining <= 0 {
			return
		}
		// Poll the stop flag at least every 500ms so shutdown stays responsive, but never wake
		// for refreshRequired.
		monitor.markAlive()
		time.Sleep(min(500*time.Millisecond, remaining))
	}
}

// Close implements Monitor interface - closes resources.
//
// Leaves the allowed/blocked host list to expire on its own TTL. Close runs from Stop on every monitor
// recreate, not only at shutdown, so deleting here would leave host selection unfiltered until the
// replacement published. Runs on the shared monitor-cleanup goroutine, which has no recover(), so it must
// stay panic-free.
func (monitor *CustomEndpointMonitorImpl) Close() {
	slog.Debug(error_util.GetMessage("CustomEndpointMonitorImpl.stoppingMonitor",
		monitor.customEndpointHostInfo.GetUrl()))
	monitor.releaseOwnedKey()
}

// isSelf reports whether the registered owner of a key is this monitor instance. Used as the
// predicate for both publication and release, so the two agree on what ownership means.
func (monitor *CustomEndpointMonitorImpl) isSelf(owner *CustomEndpointMonitorImpl) bool {
	return owner == monitor
}

// releaseOwnedKey drops the cached endpoint info, but only while this instance still owns the key.
// The ownership test, deregistration and cache removal are one atomic step: split across two calls, a
// replacement could claim the key in between and lose its entry to this delete.
func (monitor *CustomEndpointMonitorImpl) releaseOwnedKey() {
	key := monitor.getCustomEndpointInfoCacheKey()
	// A replacement owning the key, or this instance having already released it, both mean the entries
	// are not ours to remove - and in both cases RemoveIfValue does nothing.
	keyOwners.RemoveIfValue(key, func(owner *CustomEndpointMonitorImpl) bool {
		if owner != monitor {
			return false
		}
		customEndpointInfoCache.Remove(key)
		return true
	})
}

// GetLastActivityTimestampNanos implements Monitor interface.
func (monitor *CustomEndpointMonitorImpl) GetLastActivityTimestampNanos() int64 {
	return monitor.lastActivityTimestampNano.Load()
}

// GetState implements Monitor interface.
func (monitor *CustomEndpointMonitorImpl) GetState() driver_infrastructure.MonitorState {
	if state := monitor.state.Load(); state != nil {
		return state.(driver_infrastructure.MonitorState)
	}
	return driver_infrastructure.MonitorStateStopped
}

// CanDispose implements Monitor interface.
func (monitor *CustomEndpointMonitorImpl) CanDispose() bool {
	return true
}

// getCustomEndpointInfoCacheKey keys on the URL, matching the monitor-service key and the
// allowed/blocked host permissions key. Keying the info cache on the bare host while the
// permissions were keyed on host:port meant two sql.DB handles differing only by port shared one
// cache entry.
func (monitor *CustomEndpointMonitorImpl) getCustomEndpointInfoCacheKey() string {
	return monitor.customEndpointHostInfo.GetUrl()
}

// HasCustomEndpointInfo returns true if custom endpoint info is available.
func (monitor *CustomEndpointMonitorImpl) HasCustomEndpointInfo() bool {
	_, ok := customEndpointInfoCache.Get(monitor.getCustomEndpointInfoCacheKey())
	if !ok && !monitor.refreshRequired.Load() && !monitor.hasConnectionIssue.Load() {
		// There is no custom endpoint info, probably because the cache entry has expired.
		// Wake up the monitor if it is sleeping.
		monitor.RequestCustomEndpointInfoUpdate()
	}
	return ok
}

// RequestCustomEndpointInfoUpdate requests the monitor to refresh custom endpoint info.
func (monitor *CustomEndpointMonitorImpl) RequestCustomEndpointInfoUpdate() {
	if monitor.hasConnectionIssue.Load() {
		// We can't force update since there's an AWS SDK connectivity issue.
		return
	}
	monitor.refreshMu.Lock()
	monitor.refreshRequired.Store(true)
	monitor.refreshCond.Broadcast()
	monitor.refreshMu.Unlock()
}

// ClearCache clears the shared custom endpoint information cache.
func ClearCache() {
	slog.Info(error_util.GetMessage("CustomEndpointMonitorImpl.clearCache"))
	customEndpointInfoCache.Clear()
}

// CloseAllMonitors stops and removes all custom endpoint monitors.
func CloseAllMonitors(monitorService driver_infrastructure.MonitorService) {
	monitorService.StopAndRemoveByType(CustomEndpointMonitorType)
}
