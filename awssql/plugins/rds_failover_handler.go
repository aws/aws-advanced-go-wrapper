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

package plugins

import (
	"errors"
	"log/slog"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

// rdsFailoverHandler implements failoverHandler for standard Aurora and Multi-AZ RDS clusters.
type rdsFailoverHandler struct {
	plugin *FailoverPlugin
}

func NewRdsFailoverHandler(plugin *FailoverPlugin) *rdsFailoverHandler {
	return &rdsFailoverHandler{plugin: plugin}
}

func (h *rdsFailoverHandler) initFailoverMode() error {
	p := h.plugin
	if p.rdsUrlType == utils.OTHER {
		p.FailoverMode = failoverModeFromValue(strings.ToLower(property_util.GetVerifiedWrapperPropertyValue[string](p.props, property_util.FAILOVER_MODE)))
		initialHostInfo := p.servicesContainer.GetPluginService().GetInitialConnectionHostInfo()
		p.rdsUrlType = utils.IdentifyRdsUrlType(initialHostInfo.Host)

		if p.FailoverMode == MODE_UNKNOWN {
			if p.rdsUrlType == utils.RDS_READER_CLUSTER {
				p.FailoverMode = MODE_READER_OR_WRITER
			} else {
				p.FailoverMode = MODE_STRICT_WRITER
			}
		}

		slog.Debug(error_util.GetMessage("Failover.parameterValue", "failoverMode", p.FailoverMode))
	}
	return nil
}

func (h *rdsFailoverHandler) failover() error {
	p := h.plugin
	if p.FailoverMode == MODE_STRICT_WRITER {
		return p.FailoverWriter()
	} else {
		return p.FailoverReader()
	}
}

func (h *rdsFailoverHandler) dealWithError(err error) error {
	p := h.plugin
	if err != nil {
		slog.Debug(error_util.GetMessage("Failover.detectedError", err.Error()))
		if !errors.Is(err, p.lastErrorDealtWith) && p.shouldErrorTriggerConnectionSwitch(err) {
			p.InvalidateCurrentConnection()
			currentHost, e := p.servicesContainer.GetPluginService().GetCurrentHostInfo()
			if e != nil {
				return e
			}
			p.servicesContainer.GetPluginService().SetAvailability(currentHost.Aliases, host_info_util.UNAVAILABLE)
			e = h.failover()
			if e != nil {
				return e
			}
			p.lastErrorDealtWith = err
		}
	}
	return err
}
