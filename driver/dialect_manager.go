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

package driver

import (
	"awssql/utils"
	"database/sql/driver"
	"log"
	"strings"
)

var knownDialectsByCode map[string]DatabaseDialect = map[string]DatabaseDialect{
	MYSQL:        &MySQLDatabaseDialect{},
	PG:           &PgDatabaseDialect{},
	RDS_MYSQL:    &RdsMySQLDatabaseDialect{},
	RDS_PG:       &RdsPgDatabaseDialect{},
	AURORA_MYSQL: &AuroraMySQLDatabaseDialect{},
	AURORA_PG:    &AuroraPgDatabaseDialect{},
}

type DialectProvider interface {
	GetDialect(dsn string, props map[string]string) (DatabaseDialect, error)
	GetDialectForUpdate(conn driver.Conn, originalHost string, newHost string) DatabaseDialect
}

type DialectManager struct {
	canUpdate             bool
	dialect               DatabaseDialect
	dialectCode           string
	knownEndpointDialects map[string]string // TODO: update to cache map when implemented.
}

func (d *DialectManager) GetDialect(dsn string, props map[string]string) (DatabaseDialect, error) {
	// TODO: requires dsn parsing to determine what the initial dialect should be. Currently bases off of inaccurate
	// checks for phrases in the dsn.
	dialectCode := d.knownEndpointDialects[dsn]
	var userDialect DatabaseDialect
	if dialectCode != "" {
		userDialect = knownDialectsByCode[dialectCode]
		if userDialect != nil {
			d.dialectCode = dialectCode
			d.dialect = userDialect
			d.logCurrentDialect()
			return userDialect, nil
		}
	}

	driverProtocol, ok := props[utils.DRIVER_PROTOCOL]
	if !ok && driverProtocol != "" {
		return nil, NewIllegalArgumentError(GetMessage("DatabaseDialectManager.invalidDriverProtocol", driverProtocol))
	}

	hostString := dsn
	hostInfoList, err := utils.GetHostsFromDsn(dsn, true)
	if err == nil && len(hostInfoList) > 0 {
		hostString = hostInfoList[0].Host
	}
	rdsUrlType := utils.IdentifyRdsUrlType(hostString)
	if strings.Contains(driverProtocol, "mysql") {
		if rdsUrlType.IsRdsCluster {
			d.canUpdate = true
			d.dialectCode = AURORA_MYSQL
			d.dialect = knownDialectsByCode[AURORA_MYSQL]
			d.logCurrentDialect()
			return d.dialect, nil
		}
		if rdsUrlType.IsRds {
			d.canUpdate = true
			d.dialectCode = RDS_MYSQL
			d.dialect = knownDialectsByCode[RDS_MYSQL]
			d.logCurrentDialect()
			return d.dialect, nil
		}
		d.canUpdate = true
		d.dialectCode = MYSQL
		d.dialect = knownDialectsByCode[MYSQL]
		d.logCurrentDialect()
		return d.dialect, nil
	}
	if strings.Contains(driverProtocol, "postgres") {
		if rdsUrlType.IsRdsCluster {
			d.canUpdate = false
			d.dialectCode = AURORA_PG
			d.dialect = knownDialectsByCode[AURORA_PG]
			d.logCurrentDialect()
			return d.dialect, nil
		}
		if rdsUrlType.IsRds {
			d.canUpdate = true
			d.dialectCode = RDS_PG
			d.dialect = knownDialectsByCode[RDS_PG]
			d.logCurrentDialect()
			return d.dialect, nil
		}
		d.canUpdate = true
		d.dialectCode = PG
		d.dialect = knownDialectsByCode[PG]
		d.logCurrentDialect()
		return d.dialect, nil
	}
	return nil, &AwsWrapperError{GetMessage("DatabaseDialectManager.getDialectError"), 0}
}

func (d *DialectManager) GetDialectForUpdate(conn driver.Conn, originalHost string, newHost string) DatabaseDialect {
	if !d.canUpdate {
		return d.dialect
	}
	dialectCandidateCodes := d.dialect.GetDialectUpdateCandidates()
	for i := 0; i < len(dialectCandidateCodes); i++ {
		candidateCode := dialectCandidateCodes[i]
		dialectCandidate := knownDialectsByCode[candidateCode]
		if dialectCandidate != nil && dialectCandidate.IsDialect(conn) {
			d.canUpdate = false
			d.dialectCode = candidateCode
			d.dialect = dialectCandidate

			d.knownEndpointDialects[originalHost] = d.dialectCode
			d.knownEndpointDialects[newHost] = d.dialectCode

			d.logCurrentDialect()
			return d.dialect
		}
	}

	d.canUpdate = false
	d.knownEndpointDialects[originalHost] = d.dialectCode
	d.knownEndpointDialects[newHost] = d.dialectCode

	d.logCurrentDialect()
	return d.dialect
}

func (d *DialectManager) logCurrentDialect() {
	log.Printf("Current dialect: %s, %s, canUpdate: %t.\n",
		d.dialectCode,
		d.dialect,
		d.canUpdate)
}
