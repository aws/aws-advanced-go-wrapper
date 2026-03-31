/*
  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package auth_helpers

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// Aws Sts Client.
type AwsStsClient interface {
	AssumeRoleWithSAML(ctx context.Context, params *sts.AssumeRoleWithSAMLInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleWithSAMLOutput, error)
}

type AwsStsClientProvider func(region string) AwsStsClient

func NewAwsStsClient(region string) AwsStsClient {
	if region != "" {
		return sts.New(sts.Options{
			Region:      region,
			Credentials: aws.AnonymousCredentials{},
		})
	}

	// When region is empty, fall back to the default AWS config chain for region resolution
	// (env vars, ~/.aws/config, EC2 metadata, etc.) but still use anonymous credentials.
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		slog.Warn("Failed to load default AWS config, creating STS client without region", "error", err)
		return sts.New(sts.Options{Credentials: aws.AnonymousCredentials{}})
	}
	return sts.NewFromConfig(cfg, func(o *sts.Options) {
		o.Credentials = aws.AnonymousCredentials{}
	})
}
