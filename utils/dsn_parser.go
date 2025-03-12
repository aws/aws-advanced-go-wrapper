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
	"awssql/driver_infrastructure"
	"errors"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const (
	USER     = "user"
	PASSWORD = "password"
	HOST     = "host"
	PORT     = "port"
	DATABASE = "database"
	PROTOCOL = "protocol"
	NET      = "net"
)

var (
	pgxKeyValueDsnPattern = regexp.MustCompile("([a-zA-Z0-9]+=[-a-zA-Z0-9+&@#/%?=~_!:,.']+[ ]*)+")
	mySqlDsnPattern       = regexp.MustCompile(`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
		`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`\/(?P<dbname>.*?)` + // /dbname
		`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]
)

func GetHostsFromDsn(dsn string, isSingleWriterDsn bool) (hostInfoList []driver_infrastructure.HostInfo, err error) {
	// check if valid dsn... if not return empty hostlist
	properties, err := ParseDsn(dsn)
	if err != nil {
		return hostInfoList, err
	}

	hostStringList := strings.Split(properties[HOST], ",")
	portStringList := strings.Split(properties[PORT], ",")

	for i, hostString := range hostStringList {
		portString := portStringList[i]
		port := driver_infrastructure.HOST_NO_PORT
		if portString != "" {
			port, err = strconv.Atoi(portString)
			if err != nil {
				port = driver_infrastructure.HOST_NO_PORT
			}
		}

		var hostRole driver_infrastructure.HostRole
		if isSingleWriterDsn {
			hostRole = driver_infrastructure.READER
			if i == 0 {
				hostRole = driver_infrastructure.WRITER
			}
		} else {
			urlType := IdentifyRdsUrlType(hostString)
			if urlType == RDS_READER_CLUSTER {
				hostRole = driver_infrastructure.READER
			} else {
				hostRole = driver_infrastructure.WRITER
			}
		}

		builder := driver_infrastructure.NewHostInfoBuilder()
		builder.SetHost(hostString).SetPort(port).SetRole(hostRole)
		hostInfo := builder.Build()
		hostInfoList = append(hostInfoList, *hostInfo)
	}
	return
}

func ParseHostPortPair(dsn string) (driver_infrastructure.HostInfo, error) {
	hosts, err := GetHostsFromDsn(dsn, true)
	return hosts[0], err
}

func ParseDatabaseFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err != nil {
		return props[DATABASE], nil
	}
	return "", err
}

func ParseUserFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err != nil {
		return props[USER], nil
	}
	return "", err
}

func ParsePasswordFromDsn(dsn string) (string, error) {
	props, err := ParseDsn(dsn)
	if err != nil {
		return props[PASSWORD], nil
	}
	return "", err
}

func GetProtocol(dsn string) (string, error) {
	if isDsnPgxUrl(dsn) {
		return "postgresql", nil
	}

	if isDsnMySql(dsn) {
		return "mysql", nil
	}

	if isDsnPgxKeyValueString(dsn) {
		return "postgresql", nil
	}

	return "", driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.unableToDetermineProtocol", dsn))
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
		properties[USER] = parsedURL.User.Username()
		if password, present := parsedURL.User.Password(); present {
			properties[PASSWORD] = password
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
			return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.failedToSplitHostPort", host, err))
		}
		if h != "" {
			hosts = append(hosts, h)
		}
		if p != "" {
			ports = append(ports, p)
		}
	}
	if len(hosts) > 0 {
		properties[HOST] = strings.Join(hosts, ",")
	}
	if len(ports) > 0 {
		properties[PORT] = strings.Join(ports, ",")
	}

	database := strings.TrimLeft(parsedURL.Path, "/")
	if database != "" {
		properties[DATABASE] = database
	}

	nameMap := map[string]string{
		"dbname": "database",
	}

	properties[PROTOCOL] = "postgresql"

	for k, v := range parsedURL.Query() {
		if k2, present := nameMap[k]; present {
			k = k2
		}

		properties[k] = v[0]
	}

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
			return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.invalidKeyValue", dsn))
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
						return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.invalidBackslash", dsn))
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
				return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.unterminatedQuotedString", dsn))
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
			return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.invalidKeyValue", dsn))
		}

		properties[key] = val
	}

	properties[PROTOCOL] = "postgresql"

	return properties, nil
}

func parseMySqlDsn(dsn string) (properties map[string]string, err error) {
	properties = make(map[string]string)
	properties[PROTOCOL] = "mysql"

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	lastSlashIndex := strings.LastIndex(dsn, "/")
	if lastSlashIndex == -1 {
		return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.invalidDatabaseNoSlash", dsn))
	}

	// [username[:password]@][protocol[(address)]]
	lastAtIndex := strings.LastIndex(dsn[:lastSlashIndex], "@")
	if lastAtIndex != -1 {
		colonIndex := strings.Index(dsn[:lastAtIndex], ":")
		if colonIndex == -1 {
			properties[USER] = dsn[:lastAtIndex]
		} else {
			properties[USER] = dsn[:colonIndex]
			properties[PASSWORD] = dsn[colonIndex+1 : lastAtIndex]
		}
	}

	// [protocol[(address)]]
	openParenIndex := strings.Index(dsn[lastAtIndex:lastSlashIndex], "(") + lastAtIndex
	if openParenIndex == -1 {
		properties[NET] = dsn[lastAtIndex+1 : lastSlashIndex]
	} else {
		properties[NET] = dsn[lastAtIndex+1 : openParenIndex]

		closeParenIndex := strings.LastIndex(dsn[lastAtIndex:lastSlashIndex], ")") + lastAtIndex
		if closeParenIndex == -1 {
			return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.invalidAddress", dsn))
		} else if closeParenIndex < openParenIndex {
			return nil, driver_infrastructure.NewDsnParsingError(driver_infrastructure.GetMessage("DsnParser.invalidAddress", dsn))
		}
		address := dsn[openParenIndex+1 : closeParenIndex]
		hostPortPair := strings.Split(address, ":")
		properties[HOST] = hostPortPair[0]
		if len(hostPortPair) > 1 {
			properties[PORT] = hostPortPair[1]
		}
	}

	// dbname[?param1=value1&...&paramN=valueN]
	queryIndex := strings.Index(dsn[lastSlashIndex:], "?") + lastSlashIndex
	if queryIndex == -1 {
		properties[DATABASE], err = url.PathUnescape(dsn[lastSlashIndex+1:])
	} else {
		properties[DATABASE], err = url.PathUnescape(dsn[lastSlashIndex+1 : queryIndex])
		if err != nil {
			return
		}

		err = parseDSNParams(properties, dsn[queryIndex+1:])
	}

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
