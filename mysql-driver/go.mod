module github.com/aws/aws-advanced-go-wrapper/mysql-driver

go 1.25.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql/v2 v2.0.0
	github.com/go-sql-driver/mysql v1.9.3
)

require (
	filippo.io/edwards25519 v1.1.1 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	golang.org/x/text v0.35.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql/v2 => ../awssql
