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

package property_util

import (
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/host_info_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
)

const (
	PGX_DRIVER_PROTOCOL   = "postgresql"
	MYSQL_DRIVER_PROTOCOL = "mysql"
	NET_PROP_KEY          = "net"
)

var (
	pgxKeyValueDsnPattern = regexp.MustCompile("([a-zA-Z0-9]+=[-a-zA-Z0-9+&@#/%?=~_!:,.']+[ ]*)+")
	mySqlDsnPattern       = regexp.MustCompile(`^(?:(?P<user>[^:@/]+)(?::(?P<passwd>[^@]*))?@)?` + // [user[:password]@] - password can contain : / ?
		`(?:(?P<net>[^()/:?]+)?(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`(?:/(?P<dbname>[^?]*))?` + // [/dbname] - made optional
		`(?:\?(?P<params>.*))?` + // [?param1=value1&paramN=valueN]
		`$`)
	spaceBetweenWordsPattern = regexp.MustCompile(`^\s*\S+\s+\S.*$`)
)

func GetHostsFromDsn(dsn string, isSingleWriterDsn bool) (hostInfoList []*host_info_util.HostInfo, err error) {
	// check if valid dsn... if not return empty hostlist
	properties, err := ParseDsn(dsn)
	if err != nil {
		return hostInfoList, err
	}
	return GetHostsFromProps(properties, isSingleWriterDsn)
}

func GetHostsFromProps(properties *utils.RWMap[string], isSingleWriterDsn bool) (hostInfoList []*host_info_util.HostInfo, err error) {
	hostVal, _ := properties.Get(HOST.Name)
	portVal, _ := properties.Get(PORT.Name)
	hostStringList := strings.Split(hostVal, ",")
	portStringList := strings.Split(portVal, ",")
	port := host_info_util.HOST_NO_PORT
	if len(portStringList) > 1 {
		return hostInfoList, error_util.NewDsnParsingError(
			error_util.GetMessage("DsnParser.unableToMatchPortsToHosts"))
	} else if len(portStringList) == 1 {
		portString := portStringList[0]
		if portString != "" {
			port, err = strconv.Atoi(portString)
			if err != nil {
				port = host_info_util.HOST_NO_PORT
			}
		}
	}

	for i, hostString := range hostStringList {
		var hostRole host_info_util.HostRole
		if isSingleWriterDsn {
			hostRole = host_info_util.READER
			if i == 0 {
				hostRole = host_info_util.WRITER
			}
		} else {
			urlType := utils.IdentifyRdsUrlType(hostString)
			if urlType == utils.RDS_READER_CLUSTER {
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
	urlType := utils.IdentifyRdsUrlType(host)

	port := defaultPort
	var err error

	if len(hostPortPair) >= 2 {
		port, err = strconv.Atoi(hostPortPair[1])
		if err != nil {
			return nil, err
		}
	}

	hostRole := host_info_util.WRITER

	if urlType == utils.RDS_READER_CLUSTER {
		hostRole = host_info_util.READER
	}

	builder := host_info_util.NewHostInfoBuilder()
	builder.SetHost(host).SetPort(port).SetRole(hostRole)
	return builder.Build()
}

func ParseDatabaseFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err == nil {
		return DATABASE.Get(props), nil
	}
	return "", err
}

func ParseUserFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err == nil {
		return USER.Get(props), nil
	}
	return "", err
}

func ParsePasswordFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err == nil {
		return PASSWORD.Get(props), nil
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

	return "", error_util.NewDsnParsingError(
		error_util.GetMessage("DsnParser.unableToDetermineProtocol", MaskSensitiveInfoFromDsn(dsn)))
}

func ParseDsn(dsn string) (*utils.RWMap[string], error) {
	connStringSettings := utils.NewRWMap[string]()
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
	return !spaceBetweenWordsPattern.MatchString(dsn) && mySqlDsnPattern.MatchString(dsn)
}

func parsePgxURLSettings(connString string) (*utils.RWMap[string], error) {
	properties := utils.NewRWMap[string]()
	connString = strings.TrimSpace(connString)

	parsedURL, err := url.Parse(connString)
	if err != nil {
		if urlErr := new(url.Error); errors.As(err, &urlErr) {
			return nil, urlErr.Err
		}
		return nil, err
	}

	if parsedURL.User != nil {
		properties.Put(USER.Name, parsedURL.User.Username())
		if password, present := parsedURL.User.Password(); present {
			properties.Put(PASSWORD.Name, password)
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
		properties.Put(HOST.Name, strings.Join(hosts, ","))
	}
	if len(ports) > 0 {
		properties.Put(PORT.Name, strings.Join(ports, ","))
	}

	database := strings.TrimLeft(parsedURL.Path, "/")
	if database != "" {
		properties.Put(DATABASE.Name, database)
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	for k, v := range parsedURL.Query() {
		if k2, present := nameMap[k]; present {
			k = k2
		}

		properties.Put(k, v[0])
	}

	properties.Put(DRIVER_PROTOCOL.Name, PGX_DRIVER_PROTOCOL)
	return properties, nil
}

func isIPOnly(host string) bool {
	return net.ParseIP(strings.Trim(host, "[]")) != nil || !strings.Contains(host, ":")
}

var asciiSpace = [256]uint8{'\t': 1, '\n': 1, '\v': 1, '\f': 1, '\r': 1, ' ': 1}

func parsePgxKeywordValueSettings(dsn string) (*utils.RWMap[string], error) {
	properties := utils.NewRWMap[string]()

	nameMap := map[string]string{
		"dbname": "database",
	}

	for len(dsn) > 0 {
		var key, val string
		eqIdx := strings.IndexRune(dsn, '=')
		if eqIdx < 0 {
			return nil, error_util.NewDsnParsingError(
				error_util.GetMessage("DsnParser.invalidKeyValue", MaskSensitiveInfoFromDsn(dsn)))
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
						return nil, error_util.NewDsnParsingError(
							error_util.GetMessage("DsnParser.invalidBackslash", MaskSensitiveInfoFromDsn(dsn)))
					}
				}
			}
			val = strings.Replace(strings.Replace(dsn[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(dsn) {
				dsn = ""
			} else {
				dsn = strings.TrimLeft(dsn[end+1:], " \t\n\r\v\f")
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
				return nil, error_util.NewDsnParsingError(
					error_util.GetMessage("DsnParser.unterminatedQuotedString", MaskSensitiveInfoFromDsn(dsn)))
			}
			val = strings.Replace(strings.Replace(dsn[:end], "\\\\", "\\", -1), "\\'", "'", -1)
			if end == len(dsn) {
				dsn = ""
			} else {
				dsn = strings.TrimLeft(dsn[end+1:], " \t\n\r\v\f")
			}
		}

		if k, ok := nameMap[key]; ok {
			key = k
		}

		if key == "" {
			return nil, error_util.NewDsnParsingError(
				error_util.GetMessage("DsnParser.invalidKeyValue", MaskSensitiveInfoFromDsn(dsn)))
		}

		properties.Put(key, val)
	}

	properties.Put(DRIVER_PROTOCOL.Name, PGX_DRIVER_PROTOCOL)

	return properties, nil
}

func parseMySqlDsn(dsn string) (*utils.RWMap[string], error) {
	properties := utils.NewRWMap[string]()
	dsn = strings.TrimSpace(dsn)

	matches := mySqlDsnPattern.FindStringSubmatch(dsn)
	if matches == nil {
		return nil, error_util.NewDsnParsingError(
			error_util.GetMessage("DsnParser.invalidDsn", MaskSensitiveInfoFromDsn(dsn)))
	}

	names := mySqlDsnPattern.SubexpNames()
	for i, match := range matches {
		if i == 0 || names[i] == "" {
			continue
		}
		switch names[i] {
		case "user":
			if match != "" {
				properties.Put(USER.Name, match)
			}
		case "passwd":
			if match != "" {
				properties.Put(PASSWORD.Name, match)
			}
		case "net":
			if match != "" {
				properties.Put(NET_PROP_KEY, match)
			}
		case "addr":
			if match != "" {
				hostPortPair := strings.Split(match, ":")
				properties.Put(HOST.Name, hostPortPair[0])
				if len(hostPortPair) > 1 {
					properties.Put(PORT.Name, hostPortPair[1])
				}
			}
		case "dbname":
			if match != "" {
				dbname, err := url.PathUnescape(match)
				if err != nil {
					return properties, err
				}
				properties.Put(DATABASE.Name, dbname)
			}
		case "params":
			if match != "" {
				err := parseDSNParams(properties, match)
				if err != nil {
					return properties, err
				}
			}
		}
	}

	properties.Put(DRIVER_PROTOCOL.Name, MYSQL_DRIVER_PROTOCOL)
	return properties, nil
}

func parseDSNParams(properties *utils.RWMap[string], params string) error {
	for _, v := range strings.Split(params, "&") {
		key, value, ok := strings.Cut(v, "=")
		if !ok {
			continue
		}

		propVal, err := url.QueryUnescape(value)
		if err != nil {
			return err
		}
		properties.Put(key, propVal)
	}
	return nil
}
