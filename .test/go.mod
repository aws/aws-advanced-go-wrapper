module github.com/aws/aws-advanced-go-wrapper/.test

go 1.24.0

require (
	github.com/Shopify/toxiproxy v2.1.4+incompatible
	github.com/aws/aws-advanced-go-wrapper/auth-helpers v1.0.5
	github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager v1.1.0
	github.com/aws/aws-advanced-go-wrapper/awssql v1.4.0
	github.com/aws/aws-advanced-go-wrapper/custom-endpoint v1.0.2
	github.com/aws/aws-advanced-go-wrapper/federated-auth v1.0.5
	github.com/aws/aws-advanced-go-wrapper/iam v1.0.5
	github.com/aws/aws-advanced-go-wrapper/mysql-driver v1.0.5
	github.com/aws/aws-advanced-go-wrapper/okta v1.0.5
	github.com/aws/aws-advanced-go-wrapper/otlp v1.0.5
	github.com/aws/aws-advanced-go-wrapper/pgx-driver v1.0.5
	github.com/aws/aws-advanced-go-wrapper/xray v1.0.5
	github.com/aws/aws-sdk-go-v2 v1.41.1
	github.com/aws/aws-sdk-go-v2/config v1.32.7
	github.com/aws/aws-sdk-go-v2/service/rds v1.114.0
	github.com/aws/aws-sdk-go-v2/service/secretsmanager v1.41.1
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.6
	github.com/aws/aws-xray-sdk-go v1.8.5
	github.com/go-sql-driver/mysql v1.9.3
	github.com/golang/mock v1.6.0
	github.com/google/uuid v1.6.0
	github.com/olekukonko/tablewriter v1.1.3
	github.com/stretchr/testify v1.11.1
	github.com/xuri/excelize/v2 v2.10.0
	go.opentelemetry.io/contrib/propagators/aws v1.39.0
	go.opentelemetry.io/otel v1.40.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.40.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.40.0
	go.opentelemetry.io/otel/sdk v1.40.0
	go.opentelemetry.io/otel/sdk/metric v1.40.0
	go.opentelemetry.io/otel/trace v1.40.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/PuerkitoBio/goquery v1.11.0 // indirect
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/aws/aws-sdk-go v1.55.7 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.19.7 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.17 // indirect
	github.com/aws/aws-sdk-go-v2/feature/rds/auth v1.6.17
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.17 // indirect; indirectg
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.17 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.4 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.17 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.13 // indirect
	github.com/aws/smithy-go v1.24.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.7 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/pgx/v5 v5.8.0
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/nicksnyder/go-i18n/v2 v2.6.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/richardlehane/mscfb v1.0.4 // indirect
	github.com/richardlehane/msoleps v1.0.4 // indirect
	github.com/tiendc/go-deepcopy v1.7.1 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.62.0 // indirect
	github.com/xuri/efp v0.0.1 // indirect
	github.com/xuri/nfp v0.0.2-0.20250530014748-2ddeb826f9a9 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.40.0 // indirect
	go.opentelemetry.io/proto/otlp v1.9.0 // indirect
	golang.org/x/crypto v0.47.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.34.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260128011058-8636f8732409 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260128011058-8636f8732409 // indirect
	google.golang.org/grpc v1.78.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

require (
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.5 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clipperhouse/displaywidth v0.6.2 // indirect
	github.com/clipperhouse/stringish v0.1.1 // indirect
	github.com/clipperhouse/uax29/v2 v2.3.0 // indirect
	github.com/fatih/color v1.18.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.19 // indirect
	github.com/olekukonko/cat v0.0.0-20250911104152-50322a0618f6 // indirect
	github.com/olekukonko/errors v1.1.0 // indirect
	github.com/olekukonko/ll v0.1.4-0.20260115111900-9e59c2286df0 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/aws/aws-advanced-go-wrapper/awssql => ../awssql

replace github.com/aws/aws-advanced-go-wrapper/custom-endpoint => ./../custom-endpoint

replace github.com/aws/aws-advanced-go-wrapper/pgx-driver => ./../pgx-driver

replace github.com/aws/aws-advanced-go-wrapper/mysql-driver => ./../mysql-driver

replace github.com/aws/aws-advanced-go-wrapper/aws-secrets-manager => ./../aws-secrets-manager

replace github.com/aws/aws-advanced-go-wrapper/iam => ./../iam

replace github.com/aws/aws-advanced-go-wrapper/federated-auth => ./../federated-auth

replace github.com/aws/aws-advanced-go-wrapper/okta => ./../okta

replace github.com/aws/aws-advanced-go-wrapper/auth-helpers => ./../auth-helpers

replace github.com/aws/aws-advanced-go-wrapper/otlp => ./../otlp

replace github.com/aws/aws-advanced-go-wrapper/xray => ./../xray
