module examples

go 1.24.0

require (
	//github.com/aws/aws-advanced-go-wrapper/custom-endpoint v1.0.0
	github.com/aws/aws-advanced-go-wrapper/iam v1.0.2
	github.com/aws/aws-advanced-go-wrapper/mysql-driver v1.0.2
	github.com/aws/aws-advanced-go-wrapper/okta v1.0.2
	github.com/aws/aws-advanced-go-wrapper/pgx-driver v1.0.2
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/PuerkitoBio/goquery v1.10.3 // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v1.0.2 // indirect
	github.com/aws/aws-advanced-go-wrapper/awssql v1.1.1 // indirect
	github.com/aws/aws-sdk-go-v2 v1.39.4 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.31.15 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.18.19 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.11 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.7 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.11 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.11 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.29.8 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.3 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.38.9 // indirect
	github.com/aws/smithy-go v1.23.1 // indirect
	github.com/go-sql-driver/mysql v1.9.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.7.5 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.0 // indirect
	golang.org/x/crypto v0.37.0 // indirect
	golang.org/x/net v0.39.0 // indirect
	golang.org/x/sync v0.17.0 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ../auth-helpers

//replace github.com/aws/aws-advanced-go-wrapper/custom-endpoint => ../custom-endpoint

replace github.com/aws/aws-advanced-go-wrapper/iam => ../iam

replace github.com/aws/aws-advanced-go-wrapper/okta => ../okta

replace github.com/aws/aws-advanced-go-wrapper/pgx-driver => ../pgx-driver

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql
