// Package aws isolates all AWS EC2 interactions behind the EC2Client
// interface so the business logic in internal/service can be tested without
// talking to real AWS.
package aws

import (
	"context"
	"fmt"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

type InstanceState string

const (
	StateRunning  InstanceState = "running"
	StatePending  InstanceState = "pending"
	StateStopped  InstanceState = "stopped"
	StateStopping InstanceState = "stopping"
	StateUnknown  InstanceState = "unknown"
)

// Instance is a snapshot of the EC2 instance's current state and (if
// assigned) public IP.
type Instance struct {
	State    InstanceState
	PublicIP string
}

type EC2Client interface {
	Describe(ctx context.Context) (Instance, error)
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type ec2Client struct {
	client     *ec2.Client
	instanceID string
}

// NewEC2Client builds an EC2Client for the given instance, using the AWS
// default credential chain.
func NewEC2Client(ctx context.Context, region, instanceID string) (EC2Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	return &ec2Client{
		client:     ec2.NewFromConfig(cfg),
		instanceID: instanceID,
	}, nil
}

func (c *ec2Client) Describe(ctx context.Context) (Instance, error) {
	out, err := c.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		InstanceIds: []string{c.instanceID},
	})
	if err != nil {
		return Instance{}, fmt.Errorf("describe instance %s: %w", c.instanceID, err)
	}
	if len(out.Reservations) == 0 || len(out.Reservations[0].Instances) == 0 {
		return Instance{}, fmt.Errorf("instance %s not found", c.instanceID)
	}

	inst := out.Reservations[0].Instances[0]
	return Instance{
		State:    toInstanceState(inst.State.Name),
		PublicIP: awssdk.ToString(inst.PublicIpAddress),
	}, nil
}

func (c *ec2Client) Start(ctx context.Context) error {
	_, err := c.client.StartInstances(ctx, &ec2.StartInstancesInput{
		InstanceIds: []string{c.instanceID},
	})
	if err != nil {
		return fmt.Errorf("start instance %s: %w", c.instanceID, err)
	}
	return nil
}

func (c *ec2Client) Stop(ctx context.Context) error {
	_, err := c.client.StopInstances(ctx, &ec2.StopInstancesInput{
		InstanceIds: []string{c.instanceID},
	})
	if err != nil {
		return fmt.Errorf("stop instance %s: %w", c.instanceID, err)
	}
	return nil
}

func toInstanceState(name types.InstanceStateName) InstanceState {
	switch name {
	case types.InstanceStateNameRunning:
		return StateRunning
	case types.InstanceStateNamePending:
		return StatePending
	case types.InstanceStateNameStopped:
		return StateStopped
	case types.InstanceStateNameStopping:
		return StateStopping
	default:
		return InstanceState(name)
	}
}
