module github.com/aws/aws-advanced-go-wrapper/read-write-splitting

go 1.24

require github.com/aws/aws-advanced-go-wrapper/awssql v0.0.7

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/text v0.27.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql
