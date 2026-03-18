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
	"regexp"

	"github.com/aws/aws-advanced-go-wrapper/awssql/error_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/property_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/region_util"
	"github.com/aws/aws-advanced-go-wrapper/awssql/utils"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

var globalClusterIdPattern = regexp.MustCompile(
	`(?i)^(?P<instance>.+)\.(?:global-)?[a-zA-Z0-9]+\.global\.rds\.amazonaws\.com\.?$`)

type RdsClientProvider func(credentialsProvider aws.CredentialsProvider) RdsDescribeGlobalClustersClient

type RdsDescribeGlobalClustersClient interface {
	DescribeGlobalClusters(ctx context.Context, params *rds.DescribeGlobalClustersInput, optFns ...func(*rds.Options)) (*rds.DescribeGlobalClustersOutput, error)
}

type GdbRegionProvider struct {
	CredentialsProvider aws.CredentialsProvider
	RdsClientProvider   RdsClientProvider
}

func NewGdbRegionProvider(credentialsProvider aws.CredentialsProvider) *GdbRegionProvider {
	return &GdbRegionProvider{
		CredentialsProvider: credentialsProvider,
		RdsClientProvider:   defaultRdsClientProvider,
	}
}

func defaultRdsClientProvider(credentialsProvider aws.CredentialsProvider) RdsDescribeGlobalClustersClient {
	var opts []func(*config.LoadOptions) error
	if credentialsProvider != nil {
		opts = append(opts, config.WithCredentialsProvider(credentialsProvider))
	}
	cfg, err := config.LoadDefaultConfig(context.TODO(), opts...)
	if err != nil {
		slog.Warn(error_util.GetMessage("GdbRegionProvider.errorLoadingDefaultAwsConfig", err))
		return rds.New(rds.Options{})
	}
	return rds.NewFromConfig(cfg)
}

func (g *GdbRegionProvider) GetRegion(host string, props *utils.RWMap[string, string], prop property_util.AwsWrapperProperty) (region_util.Region, error) {
	region := region_util.GetRegionFromProps(props, prop)
	if region != "" {
		return region, nil
	}

	clusterId := getGlobalClusterId(host)
	if clusterId == "" {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("GdbRegionProvider.unableToParseGlobalClusterId", host))
	}

	if g.CredentialsProvider == nil {
		defaultProvider, err := getDefaultProvider(property_util.GetVerifiedWrapperPropertyValue[string](props, property_util.AWS_PROFILE))
		if err != nil {
			return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("GdbRegionProvider.errorGettingDefaultCredentials", err))
		}
		g.CredentialsProvider = defaultProvider
	}

	writerArn, err := g.findWriterClusterArn(clusterId)
	if err != nil {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("GdbRegionProvider.errorFindingWriterClusterArn", clusterId, err))
	}

	if writerArn == "" {
		return "", error_util.NewGenericAwsWrapperError(error_util.GetMessage("GdbRegionProvider.noWriterClusterFound", clusterId))
	}

	return region_util.GetRegionFromArn(writerArn), nil
}

func (g *GdbRegionProvider) findWriterClusterArn(globalClusterIdentifier string) (string, error) {
	client := g.RdsClientProvider(g.CredentialsProvider)

	output, err := client.DescribeGlobalClusters(context.TODO(), &rds.DescribeGlobalClustersInput{
		GlobalClusterIdentifier: &globalClusterIdentifier,
	})
	if err != nil {
		return "", err
	}

	for _, cluster := range output.GlobalClusters {
		for _, member := range cluster.GlobalClusterMembers {
			if aws.ToBool(member.IsWriter) {
				return aws.ToString(member.DBClusterArn), nil
			}
		}
	}

	return "", nil
}

func getGlobalClusterId(host string) string {
	matches := globalClusterIdPattern.FindStringSubmatch(host)
	idx := globalClusterIdPattern.SubexpIndex("instance")
	if idx < 0 || idx >= len(matches) {
		return ""
	}
	return matches[idx]
}
