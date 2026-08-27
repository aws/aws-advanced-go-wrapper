/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.host;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * The driver the Go wrapper is layered over for a test run.
 *
 * <p>This is a separate dimension from {@link DatabaseEngine} because PostgreSQL has more than one
 * supported target driver. It is also distinct from {@link TestDriver}, which names the JDBC driver
 * the host framework itself loads to set the cluster up.
 *
 * <p>The names must match the TargetDriver constants in the container's test_utils package, since
 * they travel to the container as the TARGET_DRIVER environment variable.
 */
public enum TargetDriver {
  PGX,
  BUN_PG,
  MYSQL;

  /**
   * Returns every driver an engine could be tested against, in the order they should run.
   *
   * <p>One provisioned cluster serves all of them: the suite is run once per driver rather than once
   * per environment, so adding a driver here costs container time and no extra database.
   */
  public static List<TargetDriver> forEngine(DatabaseEngine engine) {
    switch (engine) {
      case PG:
        return Arrays.asList(PGX, BUN_PG);
      case MYSQL:
        return Collections.singletonList(MYSQL);
      default:
        throw new IllegalArgumentException("Unknown database engine: " + engine);
    }
  }

  /**
   * Returns whether this driver should run for an engine, given the environment's features.
   *
   * <p>A driver is excluded either because it cannot serve the engine, or because a
   * {@code SKIP_..._DRIVER_TESTS} feature turned it off. The features come from the
   * {@code exclude-...-driver} switches, which is how a run is narrowed to one driver.
   */
  public boolean isAllowed(DatabaseEngine engine, Set<TestEnvironmentFeatures> features) {
    if (getEngine() != engine) {
      return false;
    }
    switch (this) {
      case PGX:
        return !features.contains(TestEnvironmentFeatures.SKIP_PG_DRIVER_TESTS);
      case BUN_PG:
        return !features.contains(TestEnvironmentFeatures.SKIP_BUNPG_DRIVER_TESTS);
      case MYSQL:
        return !features.contains(TestEnvironmentFeatures.SKIP_MYSQL_DRIVER_TESTS);
      default:
        throw new IllegalStateException("Unknown target driver: " + this);
    }
  }

  /** Returns the drivers an engine should actually be tested against for this run. */
  public static List<TargetDriver> allowedForEngine(
      DatabaseEngine engine, Set<TestEnvironmentFeatures> features) {
    final List<TargetDriver> allowed = new ArrayList<>();
    for (TargetDriver targetDriver : forEngine(engine)) {
      if (targetDriver.isAllowed(engine, features)) {
        allowed.add(targetDriver);
      }
    }
    return allowed;
  }

  /** Returns the database engine this driver connects to. */
  public DatabaseEngine getEngine() {
    switch (this) {
      case PGX:
      case BUN_PG:
        return DatabaseEngine.PG;
      case MYSQL:
        return DatabaseEngine.MYSQL;
      default:
        throw new IllegalStateException("Unknown target driver: " + this);
    }
  }

  /** Returns the driver used when a run does not ask for a specific one. */
  public static TargetDriver getDefault(DatabaseEngine engine) {
    switch (engine) {
      case PG:
        return PGX;
      case MYSQL:
        return MYSQL;
      default:
        throw new IllegalArgumentException("Unknown database engine: " + engine);
    }
  }
}
