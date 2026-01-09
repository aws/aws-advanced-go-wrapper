module github.com/aws/aws-advanced-go-wrapper/otlp

go 1.24.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql v0.0.0-20260108221638-f44e1759e32b
	go.opentelemetry.io/otel v1.39.0
	go.opentelemetry.io/otel/metric v1.39.0
	go.opentelemetry.io/otel/trace v1.39.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	golang.org/x/text v0.32.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql
