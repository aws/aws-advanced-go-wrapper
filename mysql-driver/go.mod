module github.com/aws/aws-advanced-go-wrapper/mysql-driver

go 1.24.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql v1.2.0
	github.com/go-sql-driver/mysql v1.9.3
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql
