/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package aws

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/ec2"
)

type ec2I interface {
	DescribeLaunchTemplateVersions(input *ec2.DescribeLaunchTemplateVersionsInput) (*ec2.DescribeLaunchTemplateVersionsOutput, error)
}

type ec2Wrapper struct {
	ec2I
}

func (m ec2Wrapper) getInstanceTypeByLT(launchTemplate *launchTemplate) (string, error) {
	params := &ec2.DescribeLaunchTemplateVersionsInput{
		LaunchTemplateName: aws.String(launchTemplate.name),
		Versions:           []*string{aws.String(launchTemplate.version)},
	}

	describeData, err := m.DescribeLaunchTemplateVersions(params)
	if err != nil {
		return "", err
	}

	if len(describeData.LaunchTemplateVersions) == 0 {
		return "", fmt.Errorf("unable to find template versions")
	}

	lt := describeData.LaunchTemplateVersions[0]
	instanceType := lt.LaunchTemplateData.InstanceType

	if instanceType == nil {
		return "", fmt.Errorf("unable to find instance type within launch template")
	}

	return aws.StringValue(instanceType), nil
}

func buildLaunchTemplateFromSpec(ltSpec *autoscaling.LaunchTemplateSpecification) *launchTemplate {
	// NOTE(jaypipes): The LaunchTemplateSpecification.Version is a pointer to
	// string. When the pointer is nil, EC2 AutoScaling API considers the value
	// to be "$Default", however aws.StringValue(ltSpec.Version) will return an
	// empty string (which is not considered the same as "$Default" or a nil
	// string pointer. So, in order to not pass an empty string as the version
	// for the launch template when we communicate with the EC2 AutoScaling API
	// using the information in the launchTemplate, we store the string
	// "$Default" here when the ltSpec.Version is a nil pointer.
	//
	// See:
	//
	// https://github.com/kubernetes/autoscaler/issues/1728
	// https://github.com/aws/aws-sdk-go/blob/81fad3b797f4a9bd1b452a5733dd465eefef1060/service/autoscaling/api.go#L10666-L10671
	//
	// A cleaner alternative might be to make launchTemplate.version a string
	// pointer instead of a string, or even store the aws-sdk-go's
	// LaunchTemplateSpecification structs directly.
	var version string
	if ltSpec.Version == nil {
		version = "$Default"
	} else {
		version = aws.StringValue(ltSpec.Version)
	}
	return &launchTemplate{
		name:    aws.StringValue(ltSpec.LaunchTemplateName),
		version: version,
	}
}
