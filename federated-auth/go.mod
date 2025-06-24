module github.com/aws/aws-advanced-go-wrapper/federated-auth

go 1.24

require (
	github.com/PuerkitoBio/goquery v1.10.3
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v0.0.7
	github.com/aws/aws-advanced-go-wrapper/awssql v0.0.7
)

require (
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/aws/aws-sdk-go-v2 v1.36.5 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.29.16 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.17.69 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.16.31 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.5.13 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.3.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.6.35 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.12.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.12.16 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.25.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.30.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.33.21 // indirect
	github.com/aws/smithy-go v1.22.4 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/text v0.26.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ../auth-helpers
