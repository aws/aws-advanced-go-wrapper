module github.com/aws/aws-advanced-go-wrapper/otel

go 1.24

require (
	github.com/aws/aws-advanced-go-wrapper/awssql v0.0.7
	go.opentelemetry.io/otel v1.36.0
	go.opentelemetry.io/otel/metric v1.36.0
	go.opentelemetry.io/otel/trace v1.36.0
)

require (
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	golang.org/x/text v0.26.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql
