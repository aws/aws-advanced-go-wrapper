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

import "database/sql/driver"

type PluginService interface {
	SetAvailability(hostAliases map[string]bool, availability HostAvailability)
	GetDialect() DatabaseDialect
	UpdateDialect(conn driver.Conn)
}

type PluginServiceImpl struct {
	// TODO: dialect should be initialized using DialectManager's GetDialect.
	dialect         DatabaseDialect
	dialectProvider DialectProvider
	initialHost     string
}

func (p *PluginServiceImpl) GetDialect() DatabaseDialect {
	return p.dialect
}

func (p *PluginServiceImpl) SetAvailability(hostAliases map[string]bool, availability HostAvailability) {
	// TODO: add logic for SetAvailability.
}

func (p *PluginServiceImpl) UpdateDialect(conn driver.Conn) {
	// TODO: provide information on newHost and initialHost to dialectProvider.
	newDialect := p.dialectProvider.GetDialectForUpdate(conn, p.initialHost, "")
	if p.dialect == newDialect {
		return
	}
	p.dialect = newDialect
	// TODO: update HostListProvider based on new dialect.
}
