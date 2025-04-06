package cloudwatch

import (
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/briandowns/spinner"
	"github.com/pkg/errors"
)

// ListGroups lists group names matching the specified filter
func (cwl *Client) ListGroups() (groupNames []string, err error) {
	s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
	s.Writer = os.Stderr
	s.Start()
	s.Suffix = " Fetching log groups..."
	defer s.Stop()

	groupNames = []string{}
	paginator := cloudwatchlogs.NewDescribeLogGroupsPaginator(cwl.client, &cloudwatchlogs.DescribeLogGroupsInput{})

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(context.Background())
		if err != nil {
			return nil, errors.Wrap(err, "Failed to DescribeLogGroups")
		}

		for _, group := range output.LogGroups {
			if !cwl.config.LogGroupNameFilter.MatchString(*group.LogGroupName) {
				continue
			}
			groupNames = append(groupNames, *group.LogGroupName)
		}
	}

	return groupNames, nil
}
