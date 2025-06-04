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

import com.mysql.cj.conf.PropertyKey;
import org.postgresql.PGProperty;
import org.testcontainers.shaded.org.apache.commons.lang3.NotImplementedException;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DriverHelper {


  public static String getDriverProtocol(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case PG:
        return "jdbc:postgresql://";
      case MYSQL:
        return "jdbc:mysql://";
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static String getDriverClassname(DatabaseEngine databaseEngine) {
    switch (databaseEngine) {
      case PG:
        return getDriverClassname(TestDriver.PG);
      case MYSQL:
        return getDriverClassname(TestDriver.MYSQL);
      default:
        throw new NotImplementedException(databaseEngine.toString());
    }
  }

  public static String getDriverClassname(TestDriver testDriver) {
    switch (testDriver) {
      case PG:
        return "org.postgresql.Driver";
      case MYSQL:
        return "com.mysql.cj.jdbc.Driver";
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }

  public static void setConnectTimeout(Properties props, long timeout, TimeUnit timeUnit) {
    setConnectTimeout(TestEnvironment.getCurrent().getCurrentDriver(), props, timeout, timeUnit);
  }

  public static void setConnectTimeout(
      TestDriver testDriver, Properties props, long timeout, TimeUnit timeUnit) {
    switch (testDriver) {
      case PG:
        props.setProperty(
            PGProperty.CONNECT_TIMEOUT.getName(), String.valueOf(timeUnit.toSeconds(timeout)));
        break;
      case MYSQL:
        props.setProperty(
            PropertyKey.connectTimeout.getKeyName(), String.valueOf(timeUnit.toMillis(timeout)));
        break;
      default:
        throw new NotImplementedException(testDriver.toString());
    }
  }
}
