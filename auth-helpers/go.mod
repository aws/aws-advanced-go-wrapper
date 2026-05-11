module github.com/aws/aws-advanced-go-wrapper/auth-helpers

go 1.25.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql/v2 v2.0.0
	github.com/aws/aws-sdk-go-v2 v1.41.6
	github.com/aws/aws-sdk-go-v2/config v1.32.16
	github.com/aws/aws-sdk-go-v2/credentials v1.19.15
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.22
	github.com/aws/aws-sdk-go-v2/service/rds v1.118.1
	github.com/aws/aws-sdk-go-v2/service/sts v1.42.0
)

require (
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.22 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.10 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.20 // indirect
	github.com/aws/smithy-go v1.25.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	golang.org/x/text v0.37.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql/v2 => ../awssql
