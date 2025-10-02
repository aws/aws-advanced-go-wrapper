module github.com/aws/aws-advanced-go-wrapper/federated-auth

go 1.24.0

require (
	github.com/PuerkitoBio/goquery v1.10.3
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v1.0.1
	github.com/aws/aws-advanced-go-wrapper/awssql v1.1.0
)

require (
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/aws/aws-sdk-go-v2 v1.39.0 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.31.8 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.12 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.1 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.29.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.34.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.38.4 // indirect
	github.com/aws/smithy-go v1.23.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ../auth-helpers
