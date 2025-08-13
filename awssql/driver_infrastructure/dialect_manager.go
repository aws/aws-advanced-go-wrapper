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

package driver_infrastructure

import (
	"database/sql/driver"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

var KnownDialectsByCode = map[string]DatabaseDialect{
	MYSQL_DIALECT:                      &MySQLDatabaseDialect{},
	PG_DIALECT:                         &PgDatabaseDialect{},
	RDS_MYSQL_DIALECT:                  &RdsMySQLDatabaseDialect{},
	RDS_PG_DIALECT:                     &RdsPgDatabaseDialect{},
	AURORA_MYSQL_DIALECT:               &AuroraMySQLDatabaseDialect{},
	AURORA_PG_DIALECT:                  &AuroraPgDatabaseDialect{},
	RDS_MYSQL_MULTI_AZ_CLUSTER_DIALECT: &RdsMultiAzClusterMySQLDatabaseDialect{},
	RDS_PG_MULTI_AZ_CLUSTER_DIALECT:    &RdsMultiAzClusterPgDatabaseDialect{},
}

var knownEndpointDialectsCache *utils.CacheMap[string] = utils.NewCache[string]()
var ENDPOINT_CACHE_EXPIRATION = time.Hour * 24

type DialectProvider interface {
	GetDialect(dsn string, props map[string]string) (DatabaseDialect, error)
	GetDialectForUpdate(conn driver.Conn, originalHost string, newHost string) DatabaseDialect
}

type DialectManager struct {
	canUpdate            bool
	dialect              DatabaseDialect
	dialectCode          string
	FindRegisteredDriver func(dialectCode string) bool
}

func (d *DialectManager) GetDialect(dsn string, props map[string]string) (DatabaseDialect, error) {
	if d.FindRegisteredDriver == nil {
		d.FindRegisteredDriver = utils.FindRegisteredDriver
	}

	dialectCode := property_util.DIALECT.Get(props)
	ok := true
	if dialectCode == "" {
		dialectCode, ok = knownEndpointDialectsCache.Get(dsn)
	}
	if ok && dialectCode != "" {
		userDialect := KnownDialectsByCode[dialectCode]
		if userDialect != nil {
			d.dialectCode = dialectCode
			d.dialect = userDialect
			d.logCurrentDialect()
			return userDialect, nil
		}
		return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialectManager.unknownDialectCode", dialectCode))
	}

	driverProtocol := property_util.DRIVER_PROTOCOL.Get(props)

	hostString := dsn
	hostInfoList, err := utils.GetHostsFromDsn(dsn, true)
	if err == nil && len(hostInfoList) > 0 {
		hostString = hostInfoList[0].Host
	}
	rdsUrlType := utils.IdentifyRdsUrlType(hostString)
	if strings.Contains(driverProtocol, "mysql") {
		driverIsRegistered := d.FindRegisteredDriver(AWS_MYSQL_DRIVER_CODE)
		if !driverIsRegistered {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialectManager.missingWrapperDriver", AWS_MYSQL_DRIVER_CODE))
		}

		if rdsUrlType.IsRdsCluster {
			d.canUpdate = true
			d.dialectCode = AURORA_MYSQL_DIALECT
			d.dialect = KnownDialectsByCode[AURORA_MYSQL_DIALECT]
			knownEndpointDialectsCache.Put(dsn, AURORA_MYSQL_DIALECT, ENDPOINT_CACHE_EXPIRATION)
			d.logCurrentDialect()
			return d.dialect, nil
		}
		if rdsUrlType.IsRds {
			d.canUpdate = true
			d.dialectCode = RDS_MYSQL_DIALECT
			d.dialect = KnownDialectsByCode[RDS_MYSQL_DIALECT]
			d.logCurrentDialect()
			return d.dialect, nil
		}
		d.canUpdate = true
		d.dialectCode = MYSQL_DIALECT
		d.dialect = KnownDialectsByCode[MYSQL_DIALECT]
		d.logCurrentDialect()
		return d.dialect, nil
	}
	if strings.Contains(driverProtocol, "postgres") {
		driverIsRegistered := d.FindRegisteredDriver(AWS_PGX_DRIVER_CODE)
		if !driverIsRegistered {
			return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialectManager.missingWrapperDriver", AWS_PGX_DRIVER_CODE))
		}

		if rdsUrlType.IsRdsCluster {
			d.canUpdate = true
			d.dialectCode = AURORA_PG_DIALECT
			d.dialect = KnownDialectsByCode[AURORA_PG_DIALECT]
			knownEndpointDialectsCache.Put(dsn, AURORA_PG_DIALECT, ENDPOINT_CACHE_EXPIRATION)
			d.logCurrentDialect()
			return d.dialect, nil
		}
		if rdsUrlType.IsRds {
			d.canUpdate = true
			d.dialectCode = RDS_PG_DIALECT
			d.dialect = KnownDialectsByCode[RDS_PG_DIALECT]
			d.logCurrentDialect()
			return d.dialect, nil
		}
		d.canUpdate = true
		d.dialectCode = PG_DIALECT
		d.dialect = KnownDialectsByCode[PG_DIALECT]
		d.logCurrentDialect()
		return d.dialect, nil
	}
	return nil, error_util.NewGenericAwsWrapperError(error_util.GetMessage("DatabaseDialectManager.getDialectError"))
}

func (d *DialectManager) GetDialectForUpdate(conn driver.Conn, originalHost string, newHost string) DatabaseDialect {
	if !d.canUpdate {
		return d.dialect
	}
	dialectCandidateCodes := d.dialect.GetDialectUpdateCandidates()
	for i := 0; i < len(dialectCandidateCodes); i++ {
		candidateCode := dialectCandidateCodes[i]
		dialectCandidate := KnownDialectsByCode[candidateCode]
		if dialectCandidate != nil && dialectCandidate.IsDialect(conn) {
			d.canUpdate = false
			d.dialectCode = candidateCode
			d.dialect = dialectCandidate

			knownEndpointDialectsCache.Put(originalHost, d.dialectCode, ENDPOINT_CACHE_EXPIRATION)
			knownEndpointDialectsCache.Put(newHost, d.dialectCode, ENDPOINT_CACHE_EXPIRATION)

			d.logCurrentDialect()
			return d.dialect
		}
	}

	d.canUpdate = false
	knownEndpointDialectsCache.Put(originalHost, d.dialectCode, ENDPOINT_CACHE_EXPIRATION)
	knownEndpointDialectsCache.Put(newHost, d.dialectCode, ENDPOINT_CACHE_EXPIRATION)

	d.logCurrentDialect()
	return d.dialect
}

func (d *DialectManager) logCurrentDialect() {
	slog.Info(fmt.Sprintf("Current dialect: %s, canUpdate: %t.\n",
		d.dialectCode,
		d.canUpdate))
}
