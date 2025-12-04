module github.com/aws/aws-advanced-go-wrapper/pgx-driver

go 1.24.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql v1.2.0-rc2
	github.com/jackc/pgx/v5 v5.7.5
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql
