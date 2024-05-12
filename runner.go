package gorunner

import (
	"log"
)

type Runner struct {
	*Task
	process         func() error
	processCallback func(err error) error
}

func NewRunner(taskID string) *Runner {
	return &Runner{
		Task:    newTask(taskID),
		process: nil,
	}
}

func NewRunnerWithRetryCount(taskID string, retryCount int) *Runner {
	r := NewRunner(taskID)
	r.retry = retryCount
	return r
}

func (r *Runner) AddProcess(p func() error) {
	r.process = p
}

func (r *Runner) AddProcessCallback(c func(err error) error) {
	r.processCallback = c
}

func (r *Runner) Run() error {
	if r.process != nil {
		if r.HasStarted() || r.MustInterrupt() {
			return nil
		}
		r.Task.start()
		defer r.Task.end()
		err := r.process()
		r.SetError(err)
		if r.processCallback != nil {
			go r.processCallback(err)
		}
		return err
	}
	log.Panicf("Runner %s has no process", r.ID)
	return nil
}
