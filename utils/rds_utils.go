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

package utils

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

var (
	AURORA_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?P<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.rds\\.amazonaws\\.com)$")

	AURORA_CHINA_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?P<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.rds\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.amazonaws\\.com\\.cn)$")

	AURORA_OLD_CHINA_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?P<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.rds\\.amazonaws\\.com\\.cn)$")

	AURORA_GOV_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.rds\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.(amazonaws\\.com|c2s\\.ic\\.gov|sc2s\\.sgov\\.gov))$")

	IP_V4_REGEXP = regexp.MustCompile(
		"^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){1}" +
			"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}" +
			"([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$")

	IP_V6_REGEXP = regexp.MustCompile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$")

	IP_V6_COMPRESSED_REGEXP = regexp.MustCompile("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)" +
		"::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$")

	dnsRegexpArray    = [4]*regexp.Regexp{AURORA_DNS_PATTERN, AURORA_CHINA_DNS_PATTERN, AURORA_OLD_CHINA_DNS_PATTERN, AURORA_GOV_DNS_PATTERN}
	cachedDnsRegexp   = sync.Map{}
	cachedDnsPatterns = sync.Map{}

	prepareHostFunc func(string) string
)

const (
	INSTANCE_GROUP = "instance"
	DNS_GROUP      = "dns"
	DOMAIN_GROUP   = "domain"
	REGION_GROUP   = "region"
)

func IdentifyRdsUrlType(host string) RdsUrlType {
	if host == "" {
		return OTHER
	}

	if IsIPv4(host) || IsIPV6(host) {
		return IP_ADDRESS
	} else if IsWriterClusterDns(host) {
		return RDS_WRITER_CLUSTER
	} else if IsReaderClusterDns(host) {
		return RDS_READER_CLUSTER
	} else if IsRdsCustomClusterDns(host) {
		return RDS_CUSTOM_CLUSTER
	} else if IsLimitlessDbShardGroupDns(host) {
		return RDS_AURORA_LIMITLESS_DB_SHARD_GROUP
	} else if IsRdsProxyDns(host) {
		return RDS_PROXY
	} else if IsRdsDns(host) {
		return RDS_INSTANCE
	} else {
		return OTHER
	}
}

func IsIPv4(host string) bool {
	return host != "" && IP_V4_REGEXP.MatchString(host)
}

func IsIPV6(host string) bool {
	return host != "" && (IP_V6_REGEXP.MatchString(host) || IP_V6_COMPRESSED_REGEXP.MatchString(host))
}

func IsWriterClusterDns(host string) bool {
	dnsGroup := getDnsGroup(GetPreparedHost(host))
	return strings.EqualFold(dnsGroup, "cluster-")
}

func IsReaderClusterDns(host string) bool {
	dnsGroup := getDnsGroup(GetPreparedHost(host))
	return strings.EqualFold(dnsGroup, "cluster-ro-")
}

func IsRdsCustomClusterDns(host string) bool {
	dnsGroup := getDnsGroup(GetPreparedHost(host))
	return strings.EqualFold(dnsGroup, "cluster-custom-")
}

func IsLimitlessDbShardGroupDns(host string) bool {
	dnsGroup := getDnsGroup(GetPreparedHost(host))
	return strings.EqualFold(dnsGroup, "shardgrp-")
}

func IsRdsProxyDns(host string) bool {
	dnsGroup := getDnsGroup(GetPreparedHost(host))
	return strings.HasPrefix(dnsGroup, "proxy-")
}

func IsRdsDns(host string) bool {
	if host == "" {
		return false
	}
	preparedHost := GetPreparedHost(host)

	cachedDnsRegexp, ok := findAndCacheRegexp(preparedHost)
	if !ok {
		return false
	}

	dnsGroup := cachedDnsRegexp.FindStringSubmatch(host)[cachedDnsRegexp.SubexpIndex(DNS_GROUP)]
	if dnsGroup != "" {
		cachedDnsPatterns.Store(preparedHost, dnsGroup)
	}

	return true
}

func getDnsGroup(host string) string {
	if host == "" {
		return ""
	}
	dnsGroup, ok := cachedDnsPatterns.Load(host)
	if ok {
		return dnsGroup.(string)
	}

	cachedDnsRegexp, ok := findAndCacheRegexp(host)
	if !ok {
		return ""
	}
	dnsGroup = cachedDnsRegexp.FindStringSubmatch(host)[cachedDnsRegexp.SubexpIndex(DNS_GROUP)]
	cachedDnsGroup, _ := cachedDnsPatterns.LoadOrStore(host, dnsGroup)
	return cachedDnsGroup.(string)
}

func findAndCacheRegexp(host string) (regexp.Regexp, bool) {
	val, ok := cachedDnsRegexp.Load(host)
	if ok && val != nil {
		return val.(regexp.Regexp), true
	}
	for _, dnsRegexp := range dnsRegexpArray {
		if dnsRegexp.MatchString(host) {
			val, _ := cachedDnsRegexp.LoadOrStore(host, *dnsRegexp)
			return val.(regexp.Regexp), true
		}
	}
	return regexp.Regexp{}, false
}

func SetPreparedHostFunc(newPrepareHostFunc func(string) string) {
	prepareHostFunc = newPrepareHostFunc
}

func ResetPreparedHostFunc() {
	prepareHostFunc = nil
}

func GetPreparedHost(host string) string {
	if prepareHostFunc == nil {
		return host
	}

	preparedHost := prepareHostFunc(host)
	if preparedHost == "" {
		return host
	} else {
		return preparedHost
	}
}

func GetRdsInstanceHostPattern(host string) string {
	group := getDnsGroup(GetPreparedHost(host))
	if group == "" {
		return "?"
	}
	return fmt.Sprintf("?.%s", group)
}
