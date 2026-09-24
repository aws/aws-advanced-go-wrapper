module github.com/aws/aws-advanced-go-wrapper/bun-pg-driver

go 1.26.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql/v2 v2.1.0
	github.com/stretchr/testify v1.12.1
	github.com/uptrace/bun/driver/pgdriver v1.2.18
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	github.com/puzpuzpuz/xsync/v3 v3.5.1 // indirect
	github.com/tmthrgd/go-hex v0.0.0-20190904060850-447a3041c3bc // indirect
	github.com/uptrace/bun v1.2.18 // indirect
	github.com/vmihailenco/msgpack/v5 v5.4.1 // indirect
	github.com/vmihailenco/tagparser/v2 v2.0.0 // indirect
	go.opentelemetry.io/otel v1.46.0 // indirect
	go.opentelemetry.io/otel/trace v1.46.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.52.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	mellium.im/sasl v0.3.2 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql/v2 => ../awssql
