package aws

import (
	"testing"

	sdkaws "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/stretchr/testify/assert"
)

func TestBuildLaunchTemplateFromSpec(t *testing.T) {
	assert := assert.New(t)

	units := []struct {
		name string
		in   *autoscaling.LaunchTemplateSpecification
		exp  *launchTemplate
	}{
		{
			name: "non-default, specified version",
			in: &autoscaling.LaunchTemplateSpecification{
				LaunchTemplateName: sdkaws.String("foo"),
				Version:            sdkaws.String("1"),
			},
			exp: &launchTemplate{
				name:    "foo",
				version: "1",
			},
		},
		{
			name: "non-default, specified $Latest",
			in: &autoscaling.LaunchTemplateSpecification{
				LaunchTemplateName: sdkaws.String("foo"),
				Version:            sdkaws.String("$Latest"),
			},
			exp: &launchTemplate{
				name:    "foo",
				version: "$Latest",
			},
		},
		{
			name: "specified $Default",
			in: &autoscaling.LaunchTemplateSpecification{
				LaunchTemplateName: sdkaws.String("foo"),
				Version:            sdkaws.String("$Default"),
			},
			exp: &launchTemplate{
				name:    "foo",
				version: "$Default",
			},
		},
		{
			name: "no version specified",
			in: &autoscaling.LaunchTemplateSpecification{
				LaunchTemplateName: sdkaws.String("foo"),
				Version:            nil,
			},
			exp: &launchTemplate{
				name:    "foo",
				version: "$Default",
			},
		},
	}

	for _, unit := range units {
		got := buildLaunchTemplateFromSpec(unit.in)
		assert.Equal(unit.exp, got)
	}
}
