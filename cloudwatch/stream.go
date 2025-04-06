package cloudwatch

import (
	"context"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/pkg/errors"
)

// LogStream is log stream
type LogStream struct {
	Name                *string
	LastEventTimestamp  *int64
	FirstEventTimestamp *int64
	LastIngestionTime   *int64
	UploadSequenceToken *string
}

// ListStreams lists stream names matching the specified filter
func (cwl *Client) ListStreams(ctx context.Context, groupName string, since int64) (streams []*LogStream, err error) {
	streams = []*LogStream{}
	input := &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: aws.String(groupName),
	}
	if cwl.config.LogStreamNamePrefix != "" {
		input.LogStreamNamePrefix = aws.String(cwl.config.LogStreamNamePrefix)
	} else {
		input.OrderBy = types.OrderByLastEventTime
		input.Descending = aws.Bool(true)
	}

	paginator := cloudwatchlogs.NewDescribeLogStreamsPaginator(cwl.client, input)
	hasUpdatedStream := false
	minLastIngestionTime := int64(math.MaxInt64)

	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			var resourceNotFound *types.ResourceNotFoundException
			var throttling *types.ThrottlingException
			if errors.As(err, &resourceNotFound) {
				return streams, nil
			} else if errors.As(err, &throttling) {
				time.Sleep(500 * time.Millisecond)
				return nil, nil
			}
			return nil, errors.Wrap(err, "Failed to DescribeLogStreams")
		}

		for _, stream := range output.LogStreams {
			// If there is no log event in the log stream, FirstEventTimestamp, LastEventTimestamp, LastIngestionTime, and UploadSequenceToken will be nil.
			// This activity is not officially documented.
			if stream.FirstEventTimestamp == nil || stream.LastEventTimestamp == nil || stream.LastIngestionTime == nil || stream.UploadSequenceToken == nil {
				continue
			}

			if *stream.LastIngestionTime < minLastIngestionTime {
				minLastIngestionTime = *stream.LastIngestionTime
			}
			if !cwl.config.LogStreamNameFilter.MatchString(*stream.LogStreamName) {
				continue
			}
			// Use LastIngestionTime because LastEventTimestamp is updated slowly...
			if *stream.LastIngestionTime < since {
				continue
			}
			hasUpdatedStream = true
			streams = append(streams, &LogStream{
				Name:               stream.LogStreamName,
				LastEventTimestamp: stream.LastIngestionTime,
			})
		}

		// If LogStreamNamePrefix is specified, log streams can not be sorted by LastEventTimestamp.
		if cwl.config.LogStreamNamePrefix != "" {
			break
		}
		if minLastIngestionTime >= since {
			break
		}
		if !hasUpdatedStream {
			break
		}
	}

	return streams, nil
}
