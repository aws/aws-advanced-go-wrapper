module github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager

go 1.25.0

require (
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v1.1.3
	github.com/aws/aws-advanced-go-wrapper/awssql/v2 v2.0.3
	github.com/aws/aws-sdk-go-v2/config v1.32.34
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.44.2
)

require (
	github.com/aws/aws-sdk-go-v2 v1.43.3 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.33 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.34 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.32 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.34 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.35 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.34 // indirect
	github.com/aws/aws-sdk-go-v2/service/rds v1.124.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.5.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.33.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.38.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.45.3 // indirect
	github.com/aws/smithy-go v1.27.6 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	golang.org/x/text v0.40.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql/v2 => ../awssql

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ../auth-helpers
