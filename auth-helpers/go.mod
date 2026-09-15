module github.com/aws/aws-advanced-go-wrapper/auth-helpers

go 1.26.0

require (
	github.com/aws/aws-advanced-go-wrapper/awssql/v2 v2.1.0
	github.com/aws/aws-sdk-go-v2 v1.47.0
	github.com/aws/aws-sdk-go-v2/config v1.33.4
	github.com/aws/aws-sdk-go-v2/credentials v1.20.4
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.7.3
	github.com/aws/aws-sdk-go-v2/service/rds v1.129.0
	github.com/aws/aws-sdk-go-v2/service/sts v1.50.0
)

require (
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.10.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.38.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.43.0 // indirect
	github.com/aws/smithy-go v1.28.1 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	golang.org/x/text v0.42.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql/v2 => ../awssql
