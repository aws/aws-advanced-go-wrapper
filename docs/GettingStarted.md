# Getting Started

## Minimum Requirements

Before using the AWS Advanced Go Wrapper, you must install:

- GoLang 1.25.0

If you are using the AWS Advanced Go Wrapper as part of a Go project, include one of the wrapper's available drivers as a dependency.

> [!NOTE]
> Depending on which features of the AWS Advanced Go Wrapper you use, you may have additional requirements. Please refer to this [table](./user-guide/UsingTheGoWrapper.md#plugins) for more information.

## Obtaining the AWS Advanced Go Wrapper

You can use `go get` to obtain the AWS Advanced Go Wrapper.

To obtain the Aws Advanced Go Wrapper pgx driver:
```
go get github.com/aws/aws-advanced-go-wrapper/pgx-driver@latest
```

To obtain the Aws Advanced Go Wrapper Go-MySQL-Driver driver:
```
go get github.com/aws/aws-advanced-go-wrapper/mysql-driver@latest
```

To obtain the Aws Advanced Go Wrapper Bun pgdriver driver:
```
go get github.com/aws/aws-advanced-go-wrapper/bun-pg-driver@latest
```

Each driver module registers itself when it is imported, so import it for its side effects:

```go
import (
    _ "github.com/aws/aws-advanced-go-wrapper/pgx-driver"
)
```

## Using the AWS Advanced Go Wrapper

For more detailed information about how to use and configure the AWS Advanced Go Wrapper, please visit [this page](./user-guide/UsingTheGoWrapper.md).
