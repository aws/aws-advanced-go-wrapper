module github.com/aws/aws-advanced-go-wrapper/custom-endpoint

go 1.24.0

require (
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v1.0.3
	github.com/aws/aws-advanced-go-wrapper/awssql v1.2.0
	github.com/aws/aws-sdk-go-v2 v1.41.0
	github.com/aws/aws-sdk-go-v2/config v1.31.15
	github.com/aws/aws-sdk-go-v2/service/rds v1.101.0
)

require (
	github.com/aws/aws-sdk-go-v2/credentials v1.19.5 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.16 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.16 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.12 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.5 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/text v0.32.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ../auth-helpers
