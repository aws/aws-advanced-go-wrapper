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
	"errors"
	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	PGX_DRIVER_PROTOCOL   = "postgresql"
	MYSQL_DRIVER_PROTOCOL = "mysql"
	NET_PROP_KEY          = "net"
)

var (
	pgxKeyValueDsnPattern = regexp.MustCompile("([a-zA-Z0-9]+=[-a-zA-Z0-9+&@#/%?=~_!:,.']+[ ]*)+")
	mySqlDsnPattern       = regexp.MustCompile(`^(?:(?P<user>[^:@/]+)(?::(?P<passwd>[^@/]*))?@)?` + // [user[:password]@]
		`(?:(?P<net>[^()/:?]+)?(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`/(?P<dbname>[^?]+)` + // /dbname
		`(?:\?(?P<params>.*))?` + // [?param1=value1&paramN=valueN]
		`$`)
)

func GetHostsFromDsn(dsn string, isSingleWriterDsn bool) (hostInfoList []*host_info_util.HostInfo, err error) {
	// check if valid dsn... if not return empty hostlist
	properties, err := ParseDsn(dsn)
	if err != nil {
		return hostInfoList, err
	}

	hostStringList := strings.Split(properties[property_util.HOST.Name], ",")
	portStringList := strings.Split(properties[property_util.PORT.Name], ",")

	for i, hostString := range hostStringList {
		portString := portStringList[i]
		port := host_info_util.HOST_NO_PORT
		if portString != "" {
			port, err = strconv.Atoi(portString)
			if err != nil {
				port = host_info_util.HOST_NO_PORT
			}
		}

		var hostRole host_info_util.HostRole
		if isSingleWriterDsn {
			hostRole = host_info_util.READER
			if i == 0 {
				hostRole = host_info_util.WRITER
			}
		} else {
			urlType := IdentifyRdsUrlType(hostString)
			if urlType == RDS_READER_CLUSTER {
				hostRole = host_info_util.READER
			} else {
				hostRole = host_info_util.WRITER
			}
		}

		builder := host_info_util.NewHostInfoBuilder()
		builder.SetHost(hostString).SetPort(port).SetRole(hostRole)
		hostInfo, err := builder.Build()
		if err != nil {
			return hostInfoList, err
		}
		hostInfoList = append(hostInfoList, hostInfo)
	}
	return
}

func ParseHostPortPair(instanceClusterTemplate string, defaultPort int) (*host_info_util.HostInfo, error) {
	hostPortPair := strings.Split(instanceClusterTemplate, ":")

	host := hostPortPair[0]
	urlType := IdentifyRdsUrlType(host)

	port := defaultPort
	var err error

	if len(hostPortPair) >= 2 {
		port, err = strconv.Atoi(hostPortPair[1])
		if err != nil {
			return nil, err
		}
	}

	hostRole := host_info_util.WRITER

	if urlType == RDS_READER_CLUSTER {
		hostRole = host_info_util.READER
	}

	builder := host_info_util.NewHostInfoBuilder()
	builder.SetHost(host).SetPort(port).SetRole(hostRole)
	return builder.Build()
}

func ParseDatabaseFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err != nil {
		return property_util.DATABASE.Get(props), nil
	}
	return "", err
}

func ParseUserFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err != nil {
		return property_util.USER.Get(props), nil
	}
	return "", err
}

func ParsePasswordFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err != nil {
		return property_util.PASSWORD.Get(props), nil
	}
	return "", err
}

func GetProtocol(dsn string) (string, error) {
	if isDsnPgxUrl(dsn) {
		return PGX_DRIVER_PROTOCOL, nil
	}

	if isDsnMySql(dsn) {
		return MYSQL_DRIVER_PROTOCOL, nil
	}

	if isDsnPgxKeyValueString(dsn) {
		return PGX_DRIVER_PROTOCOL, nil
	}

	return "", error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.unableToDetermineProtocol", dsn))
}

func ParseDsn(dsn string) (map[string]string, error) {
	connStringSettings := make(map[string]string)
	var err error
	if isDsnPgxUrl(dsn) {
		connStringSettings, err = parsePgxURLSettings(dsn)
		if err != nil {
			return nil, err
		}
		return connStringSettings, nil
	}

	if isDsnMySql(dsn) {
		connStringSettings, err = parseMySqlDsn(dsn)
		if err != nil {
			return nil, err
		}
		return connStringSettings, nil
	}

	if isDsnPgxKeyValueString(dsn) {
		connStringSettings, err = parsePgxKeywordValueSettings(dsn)
		if err != nil {
			return nil, err
		}
		return connStringSettings, nil
	}

	return connStringSettings, err
}

func isDsnPgxUrl(dsn string) bool {
	return strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://")
}

func isDsnPgxKeyValueString(dsn string) bool {
	return pgxKeyValueDsnPattern.MatchString(dsn)
}

func isDsnMySql(dsn string) bool {
	return mySqlDsnPattern.MatchString(dsn)
}

func parsePgxURLSettings(connString string) (map[string]string, error) {
	properties := make(map[string]string)

	parsedURL, err := url.Parse(connString)
	if err != nil {
		if urlErr := new(url.Error); errors.As(err, &urlErr) {
			return nil, urlErr.Err
		}
		return nil, err
	}

	if parsedURL.User != nil {
		properties[property_util.USER.Name] = parsedURL.User.Username()
		if password, present := parsedURL.User.Password(); present {
			properties[property_util.PASSWORD.Name] = password
		}
	}

	// Handle multiple host:port's in url.Host by splitting them into host,host,host and port,port,port.
	var hosts []string
	var ports []string
	for _, host := range strings.Split(parsedURL.Host, ",") {
		if host == "" {
			continue
		}
		if isIPOnly(host) {
			hosts = append(hosts, strings.Trim(host, "[]"))
			continue
		}
		h, p, err := net.SplitHostPort(host)
		if err != nil {
			return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.failedToSplitHostPort", host, err))
		}
		if h != "" {
			hosts = append(hosts, h)
		}
		if p != "" {
			ports = append(ports, p)
		}
	}
	if len(hosts) > 0 {
		properties[property_util.HOST.Name] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		properties[property_util.PORT.Name] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(parsedURL.Path, "/")
	if database != "" {
		properties[property_util.DATABASE.Name] = database
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	for k, v := range parsedURL.Query() {
		if k2, present := nameMap[k]; present {
			k = k2
		}

		properties[k] = v[0]
	}

	properties[property_util.DRIVER_PROTOCOL.Name] = PGX_DRIVER_PROTOCOL
	return properties, nil
}

func isIPOnly(host string) bool {
	return net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":")
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func parsePgxKeywordValueSettings(dsn string) (map[string]string, error) {
	properties := make(map[string]string)

	nameMap := map[string]string{
		"dbname": "database",
	}

	for len(dsn) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(dsn, '=')
		if eqIdx < 0 {
			return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.invalidKeyValue", dsn))
		}

		key = strings.Trim(dsn[:eqIdx], " \t\n\r\v\f")
		dsn = strings.TrimLeft(dsn[eqIdx+1:], " \t\n\r\v\f")
		if len(dsn) == 0 {
			// do nothing
		} else if dsn[0] != '\'' {
			end := 0
			for ; end < len(dsn); end++ {
				if asciiSpace[dsn[end]] == 1 {
					break
				}
				if dsn[end] == '\\' {
					end++
					if end == len(dsn) {
						return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.invalidBackslash", dsn))
					}
				}
			}
			val = strings.Replace(strings.Replace(dsn[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(dsn) {
				dsn = ""
			} else {
				dsn = dsn[end+1:]
			}
		} else { // quoted string
			dsn = dsn[1:]
			end := 0
			for ; end < len(dsn); end++ {
				if dsn[end] == '\'' {
					break
				}
				if dsn[end] == '\\' {
					end++
				}
			}
			if end == len(dsn) {
				return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.unterminatedQuotedString", dsn))
			}
			val = strings.Replace(strings.Replace(dsn[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(dsn) {
				dsn = ""
			} else {
				dsn = dsn[end+1:]
			}
		}

		if k, ok := nameMap[key]; ok {
			key = k
		}

		if key == "" {
			return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.invalidKeyValue", dsn))
		}

		properties[key] = val
	}

	properties[property_util.DRIVER_PROTOCOL.Name] = PGX_DRIVER_PROTOCOL

	return properties, nil
}

func parseMySqlDsn(dsn string) (properties map[string]string, err error) {
	properties = make(map[string]string)

	// To account for props that have "/" in their value like endpoints
	paramsStartIndex := strings.Index(dsn, "?")
	if paramsStartIndex == -1 {
		paramsStartIndex = len(dsn)
	}

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]

	lastSlashIndex := strings.LastIndex(dsn[:paramsStartIndex], "/")

	if lastSlashIndex == -1 {
		return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.invalidDatabaseNoSlash", dsn))
	}

	// [username[:password]@][protocol[(address)]]
	lastAtIndex := strings.LastIndex(dsn[:lastSlashIndex], "@")
	if lastAtIndex != -1 {
		colonIndex := strings.Index(dsn[:lastAtIndex], ":")
		if colonIndex == -1 {
			properties[property_util.USER.Name] = dsn[:lastAtIndex]
		} else {
			properties[property_util.USER.Name] = dsn[:colonIndex]
			properties[property_util.PASSWORD.Name] = dsn[colonIndex+1 : lastAtIndex]
		}
	}

	// [protocol[(address)]]
	openParenIndex := strings.Index(dsn[lastAtIndex:lastSlashIndex], "(") + lastAtIndex
	if openParenIndex == -1 {
		properties[NET_PROP_KEY] = dsn[lastAtIndex+1 : lastSlashIndex]
	} else {
		properties[NET_PROP_KEY] = dsn[lastAtIndex+1 : openParenIndex]

		closeParenIndex := strings.LastIndex(dsn[lastAtIndex:lastSlashIndex], ")") + lastAtIndex
		if closeParenIndex == -1 {
			return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.invalidAddress", dsn))
		} else if closeParenIndex < openParenIndex {
			return nil, error_util.NewDsnParsingError(error_util.GetMessage("DsnParser.invalidAddress", dsn))
		}
		address := dsn[openParenIndex+1 : closeParenIndex]
		hostPortPair := strings.Split(address, ":")
		properties[property_util.HOST.Name] = hostPortPair[0]
		if len(hostPortPair) > 1 {
			properties[property_util.PORT.Name] = hostPortPair[1]
		}
	}

	// dbname[?param1=value1&...&paramN=valueN]
	queryIndex := strings.Index(dsn[lastSlashIndex:], "?") + lastSlashIndex
	if queryIndex <= lastSlashIndex {
		properties[property_util.DATABASE.Name], err = url.PathUnescape(dsn[lastSlashIndex+1:])
	} else {
		properties[property_util.DATABASE.Name], err = url.PathUnescape(dsn[lastSlashIndex+1 : queryIndex])
		if err != nil {
			return
		}

		err = parseDSNParams(properties, dsn[queryIndex+1:])
	}

	properties[property_util.DRIVER_PROTOCOL.Name] = MYSQL_DRIVER_PROTOCOL
	return
}

func parseDSNParams(properties map[string]string, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		key, value, ok := strings.Cut(v, "=")
		if !ok {
			continue
		}

		properties[key], err = url.QueryUnescape(value)
		if err != nil {
			return
		}
	}
	return
}
