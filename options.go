package gorunner

import (
	"strconv"
	"time"
)

type engineOptions struct {
	maxSimultaneousRunner int
	maxRetry              int
	name                  string
	retryInterval         time.Duration
	removeFromHistoryIf   func(taskID string, lastExecutionTime time.Time) bool
}

func NewEngineOptions() *engineOptions {
	return &engineOptions{
		maxSimultaneousRunner: 1,
		maxRetry:              3,
		name:                  strconv.Itoa(ID),
		retryInterval:         time.Second,
		removeFromHistoryIf:   func(taskID string, lastExecutionTime time.Time) bool { return false },
	}
}

func (o *engineOptions) SetRemoveFromHistoryIf(removeFromHistoryIf func(taskID string, lastExecutionTime time.Time) bool) *engineOptions {
	o.removeFromHistoryIf = removeFromHistoryIf
	return o
}

func (o *engineOptions) SetMaxSimultaneousRunner(maxSimultaneousRunner int) *engineOptions {
	o.maxSimultaneousRunner = maxSimultaneousRunner
	return o
}

func (o *engineOptions) SetMaxRetry(maxRetry int) *engineOptions {
	o.maxRetry = maxRetry
	return o
}

func (o *engineOptions) SetName(name string) *engineOptions {
	o.name = name
	return o
}

func (o *engineOptions) SetRetryInterval(retryInterval time.Duration) *engineOptions {
	o.retryInterval = retryInterval
	return o
}
