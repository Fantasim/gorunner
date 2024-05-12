package gorunner

import (
	"log"
)

type Runner struct {
	*Task
	process         func() error
	processCallback func(runner *Runner)
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

func (r *Runner) AddProcess(p func() error) *Runner {
	r.process = p
	return r
}

func (r *Runner) AddProcessCallback(c func(runner *Runner)) *Runner {
	r.processCallback = c
	return r
}

func (r *Runner) Run() error {
	if r.process != nil {
		if r.HasStarted() || r.MustInterrupt() {
			return nil
		}
		r.Task.start()
		err := r.process()
		r.Task.end()
		r.setError(err)
		if r.processCallback != nil {
			go r.processCallback(r)
		}
		return err
	}
	log.Panicf("Runner %s has no process", r.ID)
	return nil
}
