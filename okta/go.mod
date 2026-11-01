module github.com/aws/aws-advanced-go-wrapper/okta

go 1.26.0

require (
	github.com/PuerkitoBio/goquery v1.13.0
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v1.1.5
	github.com/aws/aws-advanced-go-wrapper/awssql/v2 v2.1.1
	github.com/aws/aws-sdk-go-v2 v1.47.5
)

require (
	github.com/andybalholm/cascadia v1.3.4 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.33.12 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.20.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.20.5 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.7.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.8.8 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.5.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.23 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.14.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/rds v1.131.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.12.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.39.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.44.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.51.6 // indirect
	github.com/aws/smithy-go v1.28.7 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.7.0 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/text v0.43.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql/v2 => ../awssql

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ../auth-helpers
