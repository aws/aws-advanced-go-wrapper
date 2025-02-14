package utils

import (
	"awssql/driver"
	"errors"
	"fmt"
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
)

var (
	pgxKeyValueDsnPattern = regexp.MustCompile("([a-zA-Z0-9]+=[-a-zA-Z0-9+&@#/%?=~_!:,.']+[ ]*)+")
	mySqlDsnPattern       = regexp.MustCompile(`^(?:(?P<user>.*?)(?::(?P<passwd>.*))?@)?` + // [user[:password]@]
		`(?:(?P<net>[^\(]*)(?:\((?P<addr>[^\)]*)\))?)?` + // [net[(addr)]]
		`\/(?P<dbname>.*?)` + // /dbname
		`(?:\?(?P<params>[^\?]*))?$`) // [?param1=value1&paramN=valueN]
)

func GetHostsFromDsn(dsn string, isSingleWriterDsn bool) (hostInfoList []driver.HostInfo, err error) {
	// check if valid dsn... if not return empty hostlist
	properties, err := ParseDsn(dsn)
	if err != nil {
		return hostInfoList, err
	}

	hostStringList := strings.Split(properties[HOST], ",")
	portStringList := strings.Split(properties[PORT], ",")

	for i, hostString := range hostStringList {
		portString := portStringList[i]
		port := driver.HOST_NO_PORT
		if portString != "" {
			port, err = strconv.Atoi(portString)
			if err != nil {
				port = driver.HOST_NO_PORT
			}
		}

		var hostRole driver.HostRole
		if isSingleWriterDsn {
			hostRole = driver.READER
			if i == 0 {
				hostRole = driver.WRITER
			}
		} else {
			urlType := IdentifyRdsUrlType(hostString)
			if urlType == RDS_READER_CLUSTER {
				hostRole = driver.READER
			} else {
				hostRole = driver.WRITER
			}
		}

		builder := driver.NewHostInfoBuilder()
		builder.SetHost(hostString).SetPort(port).SetRole(hostRole)
		hostInfo, err := builder.Build()
		if err == nil {
			hostInfoList = append(hostInfoList, *hostInfo)
		} else {
			panic(err)
		}
	}
	return
}

func ParseHostPortPair(dsn string) (driver.HostInfo, error) {
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

	return "", errors.New(driver.GetMessage("DsnParser.unableToDetermineProtocol", dsn))
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
			return nil, fmt.Errorf("failed to split host:port in '%s', err: %w", host, err)
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
			return nil, errors.New(driver.GetMessage("DsnParser.invalidKeyValue", dsn))
		}

		key = strings.Trim(dsn[:eqIdx], " \t\n\r\v\f")
		dsn = strings.TrimLeft(dsn[eqIdx+1:], " \t\n\r\v\f")
		if len(dsn) == 0 {
		} else if dsn[0] != '\'' {
			end := 0
			for ; end < len(dsn); end++ {
				if asciiSpace[dsn[end]] == 1 {
					break
				}
				if dsn[end] == '\\' {
					end++
					if end == len(dsn) {
						return nil, errors.New(driver.GetMessage("DsnParser.invalidBackslash", dsn))
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
				return nil, errors.New(driver.GetMessage("DsnParser.unterminatedQuotedString", dsn))
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
			return nil, errors.New(driver.GetMessage("DsnParser.invalidKeyValue", dsn))
		}

		properties[key] = val
	}

	properties[PROTOCOL] = "postgresql"

	return properties, nil
}

func parseMySqlDsn(dsn string) (properties map[string]string, err error) {
	// New config with some default values
	properties = make(map[string]string)

	// [user[:password]@][net[(addr)]]/dbname[?param1=value1&paramN=valueN]
	// Find the last '/' (since the password or the net addr might contain a '/')
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			var j, k int

			// left part is empty if i <= 0
			if i > 0 {
				// [username[:password]@][protocol[(address)]]
				// Find the last '@' in dsn[:i]
				for j = i; j >= 0; j-- {
					if dsn[j] == '@' {
						// username[:password]
						// Find the first ':' in dsn[:j]
						for k = 0; k < j; k++ {
							if dsn[k] == ':' {
								properties[PASSWORD] = dsn[k+1 : j]
								break
							}
						}
						properties[USER] = dsn[:k]
						break
					}
				}

				// [protocol[(address)]]
				// Find the first '(' in dsn[j+1:i]
				for k = j + 1; k < i; k++ {
					if dsn[k] == '(' {
						// dsn[i-1] must be == ')' if an address is specified
						if dsn[i-1] != ')' {
							if strings.ContainsRune(dsn[k+1:i], ')') {
								return nil, errors.New(driver.GetMessage("DsnParser.invalidUnescaped", dsn))
							}
							return nil, errors.New(driver.GetMessage("DsnParser.invalidAddress", dsn))
						}

						address := dsn[k+1 : i-1]
						hostPortPair := strings.Split(address, ":")
						properties[HOST] = hostPortPair[0]
						if len(hostPortPair) == 2 {
							properties[PORT] = hostPortPair[1]
						}
						break
					}
				}
				properties["net"] = dsn[j+1 : k]
			}

			// dbname[?param1=value1&...&paramN=valueN]
			// Find the first '?' in dsn[i+1:]
			for j = i + 1; j < len(dsn); j++ {
				if dsn[j] == '?' {
					if err = parseDSNParams(properties, dsn[j+1:]); err != nil {
						return
					}
					break
				}
			}

			dbname := dsn[i+1 : j]
			if properties[DATABASE], err = url.PathUnescape(dbname); err != nil {
				return nil, fmt.Errorf("invalid dbname %q: %w", dbname, err)
			}

			break
		}
	}

	properties[PROTOCOL] = "mysql"

	if !foundSlash && len(dsn) > 0 {
		return nil, errors.New(driver.GetMessage("DsnParser.invalidDatabaseNoSlash", dsn))
	}

	return
}

func parseDSNParams(properties map[string]string, params string) (err error) {
	for _, v := range strings.Split(params, "&") {
		key, value, found := strings.Cut(v, "=")
		if !found {
			continue
		}

		if properties[key], err = url.QueryUnescape(value); err != nil {
			return
		}
	}
	return
}
