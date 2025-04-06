package cloudwatch

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/briandowns/spinner"
	c "github.com/fatih/color"
	"github.com/knqyf263/utern/cache"
	"github.com/knqyf263/utern/color"
	cfg "github.com/knqyf263/utern/config"
	"github.com/pkg/errors"
)

var seenGroup = new(sync.Map)
var seenStream = new(sync.Map)

// Client represents CloudWatch Logs client
type Client struct {
	client *cloudwatchlogs.Client
	config *cfg.Config
}

type logEvent struct {
	logGroupName string
	event        *types.FilteredLogEvent
}

// NewClient creates a new instance of the CLoudWatch Logs client
func NewClient(conf *cfg.Config) *Client {
	ctx := context.Background()
	var opts []func(*awsconfig.LoadOptions) error

	log.Printf("[DEBUG] Using AWS profile: %s", conf.Profile)
	log.Printf("[DEBUG] Using AWS region: %s", conf.Region)

	if conf.Profile != "" {
		opts = append(opts, awsconfig.WithSharedConfigProfile(conf.Profile))
	}
	if conf.Region != "" {
		opts = append(opts, awsconfig.WithRegion(conf.Region))
	}
	// AWS SSOは自動的にサポートされます

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	return &Client{
		client: cloudwatchlogs.NewFromConfig(awsCfg),
		config: conf,
	}
}

// Tail tails log
func (cwl *Client) Tail(ctx context.Context) error {
	if cwl.config.Color {
		c.NoColor = false
	}
	start := make(chan struct{}, 1)
	ch := make(chan *logEvent, 1000)
	errch := make(chan error)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				cwl.print(event)
			}
		}
	}()

	go func() {
		apiTicker := time.NewTicker(250 * time.Millisecond)
		for range apiTicker.C {
			start <- struct{}{}
		}
	}()

	logGroupNames, err := cwl.ListGroups()
	if err != nil {
		return errors.Wrap(err, "Failed to list log groups")
	}

	for _, logGroupName := range logGroupNames {
		cwl.showNewGroup(logGroupName)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-start:
		}

		s := spinner.New(spinner.CharSets[14], 100*time.Millisecond)
		s.Writer = os.Stderr
		s.Start()
		s.Suffix = " Fetching log streams..."

		streams, err := cwl.ListStreams(ctx, logGroupName, cwl.config.StartTime.Unix()*1000)
		if err != nil {
			return errors.Wrap(err, "Initial check failed")
		}
		s.Stop()

		for _, stream := range streams {
			cwl.showNewStream(logGroupName, stream)
		}
	}

	wg := &sync.WaitGroup{}
	for _, logGroupName := range logGroupNames {
		wg.Add(1)
		go func(groupName string) {
			defer wg.Done()
			if err := cwl.tail(ctx, groupName, start, ch, errch); err != nil {
				errch <- err
			}
		}(logGroupName)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		for {
			if len(ch) == 0 {
				close(done)
				break
			}
		}
	}()

	select {
	case <-ctx.Done():
		close(ch)
		return ctx.Err()
	case err := <-errch:
		return err
	case <-done:
	}
	return nil
}

func (cwl *Client) tail(ctx context.Context, logGroupName string,
	start chan struct{}, ch chan *logEvent, errch chan error) error {
	lastEventTime := aws.Int64(cwl.config.StartTime.UTC().Unix() * 1000)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-start:
		}

		streams, err := cwl.ListStreams(ctx, logGroupName, *lastEventTime)
		if err != nil {
			return err
		}

		streamNames := []string{}
		for _, stream := range streams {
			streamNames = append(streamNames, *stream.Name)
			cwl.showNewStream(logGroupName, stream)
		}

		if len(streamNames) == 0 {
			continue
		}

		if len(streamNames) > 100 {
			return errors.New("log streams exceed 100, so please filter log streams with '--stream' or '--stream-prefix' option")
		}

		input := &cloudwatchlogs.FilterLogEventsInput{
			LogGroupName:   aws.String(logGroupName),
			LogStreamNames: streamNames,
			Interleaved:    aws.Bool(true),
			StartTime:      aws.Int64(*lastEventTime),
		}
		if cwl.config.FilterPattern != "" {
			input.FilterPattern = aws.String(cwl.config.FilterPattern)
		}
		if cwl.config.EndTime != nil {
			input.EndTime = aws.Int64(cwl.config.EndTime.UTC().Unix() * 1000)
		}

		paginator := cloudwatchlogs.NewFilterLogEventsPaginator(cwl.client, input)
		for paginator.HasMorePages() {
			output, err := paginator.NextPage(ctx)
			if err != nil {
				var throttling *types.ThrottlingException
				if errors.As(err, &throttling) {
					log.Printf("Rate exceeded for %s. Wait for 500ms then retry.\n", logGroupName)
					time.Sleep(500 * time.Millisecond)
					continue
				}
				return errors.Wrap(err, "Unknown error while FilterLogEvents")
			}

			for _, event := range output.Events {
				if cache.Cache.Load(logGroupName, event.EventId) {
					continue
				}
				cache.Cache.Store(logGroupName, event.EventId, event.Timestamp)
				ch <- &logEvent{
					logGroupName: logGroupName,
					event:        &event,
				}

				if *event.Timestamp > *lastEventTime {
					lastEventTime = event.Timestamp
				}
			}

			if !paginator.HasMorePages() {
				cache.Cache.Expire(logGroupName, lastEventTime)
			}
		}

		if cwl.config.EndTime != nil {
			return nil
		}
	}
}

func (cwl *Client) showNewGroup(groupName string) {
	if _, ok := seenGroup.Load(groupName); ok {
		return
	}
	g := color.GroupColor.Get(groupName).SprintFunc()
	p := c.New(c.FgHiGreen, c.Bold).SprintFunc()
	fmt.Fprintf(os.Stderr, "%s %s\n", p("+"), g(groupName))
	seenGroup.Store(groupName, struct{}{})
}

func (cwl *Client) showNewStream(groupName string, stream *LogStream) {
	groupStream := groupName + "+++" + *stream.Name
	if _, ok := seenStream.Load(groupStream); ok {
		return
	}
	g := color.GroupColor.Get(groupName).SprintFunc()
	s := color.StreamColor.Get(*stream.Name).SprintFunc()
	p := c.New(c.FgHiGreen, c.Bold).SprintFunc()
	t := formatUnixTime(*stream.LastEventTimestamp)
	fmt.Fprintf(os.Stderr, "%s %s › %s (%s)\n", p("+"), g(groupName), s(*stream.Name), t)
	seenStream.Store(groupStream, struct{}{})
}

func (cwl *Client) print(event *logEvent) {
	g := color.GroupColor.Get(event.logGroupName).SprintFunc()
	s := color.StreamColor.Get(*event.event.LogStreamName).SprintFunc()
	messages := []string{}
	if !cwl.config.NoLogGroupName {
		messages = append(messages, g(event.logGroupName))
	}
	if !cwl.config.NoLogStreamName {
		messages = append(messages, s(*event.event.LogStreamName))
	}
	if cwl.config.EventID {
		messages = append(messages, *event.event.EventId)
	}
	if cwl.config.Timestamps {
		t := formatUnixTime(*event.event.IngestionTime)
		messages = append(messages, t)
	}
	message := *event.event.Message
	if 0 < cwl.config.MaxLength && cwl.config.MaxLength < len(message) {
		message = message[:cwl.config.MaxLength]
	}
	messages = append(messages, message)
	fmt.Printf("%s\n", strings.Join(messages, " "))
}

func formatUnixTime(unixtime int64) string {
	sec := unixtime / 1000
	msec := unixtime % 1000
	t := time.Unix(sec, msec*1000)
	t = t.In(time.Local)
	return t.Format(time.RFC3339)
}
