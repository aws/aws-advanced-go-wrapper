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
			"\\.rds\\.amazonaws\\.com\\.?)$")

	AURORA_CHINA_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?P<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.rds\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.amazonaws\\.com\\.cn\\.?)$")

	AURORA_OLD_CHINA_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?P<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.rds\\.amazonaws\\.com\\.cn\\.?)$")

	AURORA_GOV_DNS_PATTERN = regexp.MustCompile(
		"(?i)^(?<instance>.+)\\." +
			"(?P<dns>proxy-|cluster-|cluster-ro-|cluster-custom-|shardgrp-)?" +
			"(?P<domain>[a-zA-Z0-9]+\\.rds\\.(?P<region>[a-zA-Z0-9\\-]+)" +
			"\\.(amazonaws\\.com\\.?|c2s\\.ic\\.gov\\.?|sc2s\\.sgov\\.gov\\.?))$")

	IP_V4_REGEXP = regexp.MustCompile(
		"^(([1-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){1}" +
			"(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){2}" +
			"([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$")

	IP_V6_REGEXP = regexp.MustCompile("^[0-9a-fA-F]{1,4}(:[0-9a-fA-F]{1,4}){7}$")

	IP_V6_COMPRESSED_REGEXP = regexp.MustCompile("^(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)" +
		"::(([0-9A-Fa-f]{1,4}(:[0-9A-Fa-f]{1,4}){0,5})?)$")

	BG_OLD_HOST_PATTERN     = regexp.MustCompile("(?i).*(?P<prefix>-old1\\.)...*") //nolint:all
	BG_GREEN_HOSTID_PATTERN = regexp.MustCompile("(?i)(.*)-green-[0-9a-z]{6}")
	BG_GREEN_HOST_PATTERN   = regexp.MustCompile("(?i).*(?P<prefix>-green-[0-9a-z]{6})...*")

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

func IsIP(host string) bool {
	return IsIPv4(host) || IsIPV6(host)
}

func IsIPv4(host string) bool {
	return host != "" && IP_V4_REGEXP.MatchString(host)
}

func IsIPV6(host string) bool {
	return host != "" && (IP_V6_REGEXP.MatchString(host) || IP_V6_COMPRESSED_REGEXP.MatchString(host))
}

func IsRdsClusterDns(host string) bool {
	dnsGroup := getDnsGroup(GetPreparedHost(host))
	return strings.EqualFold(dnsGroup, "cluster-") || strings.EqualFold(dnsGroup, "cluster-ro-")
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

	dnsGroup := cachedDnsRegexp.FindStringSubmatch(preparedHost)[cachedDnsRegexp.SubexpIndex(DNS_GROUP)]
	if dnsGroup != "" {
		cachedDnsPatterns.Store(preparedHost, dnsGroup)
	}

	return true
}

func IsRdsInstance(host string) bool {
	preparedHost := GetPreparedHost(host)

	return getDnsGroup(preparedHost) == "" && IsRdsDns(preparedHost)
}

func IsGreenInstance(host string) bool {
	preparedHost := GetPreparedHost(host)
	return preparedHost != "" && BG_GREEN_HOST_PATTERN.MatchString(preparedHost)
}
func IsNotOldInstance(host string) bool {
	preparedHost := GetPreparedHost(host)
	return preparedHost == "" || !BG_OLD_HOST_PATTERN.MatchString(preparedHost)
}

// IsNotGreenAndNotOldInstance Verify host contains neither green prefix nor old prefix.
func IsNotGreenAndNotOldInstance(host string) bool {
	preparedHost := GetPreparedHost(host)
	return preparedHost != "" && !BG_GREEN_HOST_PATTERN.MatchString(preparedHost) && !BG_OLD_HOST_PATTERN.MatchString(preparedHost)
}

func RemoveGreenInstancePrefix(host string) string {
	preparedHost := GetPreparedHost(host)
	if preparedHost == "" {
		return host
	}
	// First try the main green host pattern to extract the prefix
	if matches := BG_GREEN_HOST_PATTERN.FindStringSubmatch(preparedHost); matches != nil {
		prefixIndex := BG_GREEN_HOST_PATTERN.SubexpIndex("prefix")
		if prefixIndex >= 0 && prefixIndex < len(matches) {
			prefix := matches[prefixIndex]
			if prefix != "" {
				return strings.Replace(host, prefix+".", ".", 1)
			}
		}
		return host
	}
	// Fallback to the hostid pattern for cases where the main pattern doesn't match
	if matches := BG_GREEN_HOSTID_PATTERN.FindStringSubmatch(preparedHost); len(matches) > 1 {
		return matches[1] // Return the captured group (everything before -green-[hash])
	}

	return host
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

func getDomainGroup(host string) string {
	if host == "" {
		return ""
	}

	cachedDnsRegexp, ok := findAndCacheRegexp(host)
	if !ok {
		return ""
	}
	return cachedDnsRegexp.FindStringSubmatch(host)[cachedDnsRegexp.SubexpIndex(DOMAIN_GROUP)]
}

func GetRdsRegion(host string) string {
	preparedHost := GetPreparedHost(host)
	if preparedHost == "" {
		return ""
	}

	cachedDnsRegexp, ok := findAndCacheRegexp(preparedHost)
	if !ok {
		return ""
	}

	return cachedDnsRegexp.FindStringSubmatch(host)[cachedDnsRegexp.SubexpIndex(REGION_GROUP)]
}

func GetRdsClusterId(host string) string {
	preparedHost := GetPreparedHost(host)
	if preparedHost == "" {
		return ""
	}

	cachedDnsRegexp, ok := findAndCacheRegexp(preparedHost)
	if ok && cachedDnsRegexp.FindStringSubmatch(host)[cachedDnsRegexp.SubexpIndex(REGION_GROUP)] != "" {
		return cachedDnsRegexp.FindStringSubmatch(host)[cachedDnsRegexp.SubexpIndex(INSTANCE_GROUP)]
	}
	return ""
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
	group := getDomainGroup(GetPreparedHost(host))
	if group == "" {
		return "?"
	}
	return fmt.Sprintf("?.%s", group)
}

func GetRdsClusterHostUrl(host string) string {
	preparedHost := GetPreparedHost(host)
	if preparedHost == "" {
		return ""
	}

	if AURORA_DNS_PATTERN.MatchString(preparedHost) {
		matches := AURORA_DNS_PATTERN.FindStringSubmatch(preparedHost)
		instanceIndex := AURORA_DNS_PATTERN.SubexpIndex(INSTANCE_GROUP)
		domainIndex := AURORA_DNS_PATTERN.SubexpIndex(DOMAIN_GROUP)

		if instanceIndex >= 0 && domainIndex >= 0 && len(matches) > domainIndex {
			instance := matches[instanceIndex]
			domain := matches[domainIndex]
			return fmt.Sprintf("%s.cluster-%s", instance, domain)
		}
	}

	if AURORA_CHINA_DNS_PATTERN.MatchString(preparedHost) {
		matches := AURORA_CHINA_DNS_PATTERN.FindStringSubmatch(preparedHost)
		instanceIndex := AURORA_CHINA_DNS_PATTERN.SubexpIndex(INSTANCE_GROUP)
		domainIndex := AURORA_CHINA_DNS_PATTERN.SubexpIndex(DOMAIN_GROUP)

		if instanceIndex >= 0 && domainIndex >= 0 && len(matches) > domainIndex {
			instance := matches[instanceIndex]
			domain := matches[domainIndex]
			return fmt.Sprintf("%s.cluster-%s", instance, domain)
		}
	}

	if AURORA_OLD_CHINA_DNS_PATTERN.MatchString(preparedHost) {
		matches := AURORA_OLD_CHINA_DNS_PATTERN.FindStringSubmatch(preparedHost)
		instanceIndex := AURORA_OLD_CHINA_DNS_PATTERN.SubexpIndex(INSTANCE_GROUP)
		domainIndex := AURORA_OLD_CHINA_DNS_PATTERN.SubexpIndex(DOMAIN_GROUP)

		if instanceIndex >= 0 && domainIndex >= 0 && len(matches) > domainIndex {
			instance := matches[instanceIndex]
			domain := matches[domainIndex]
			return fmt.Sprintf("%s.cluster-%s", instance, domain)
		}
	}

	if AURORA_GOV_DNS_PATTERN.MatchString(preparedHost) {
		matches := AURORA_GOV_DNS_PATTERN.FindStringSubmatch(preparedHost)
		instanceIndex := AURORA_GOV_DNS_PATTERN.SubexpIndex(INSTANCE_GROUP)
		domainIndex := AURORA_GOV_DNS_PATTERN.SubexpIndex(DOMAIN_GROUP)

		if instanceIndex >= 0 && domainIndex >= 0 && len(matches) > domainIndex {
			instance := matches[instanceIndex]
			domain := matches[domainIndex]
			return fmt.Sprintf("%s.cluster-%s", instance, domain)
		}
	}

	return ""
}
