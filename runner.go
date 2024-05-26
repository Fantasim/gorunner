package gorunner

import (
	"errors"
	"log"
)

type Runner struct {
	*Task
	process         func() error
	processCallback func(engine *Engine, runner *Runner)
	runningFilter   func(runnings []*Runner, currentRunner *Runner) bool
}

func NewRunner(taskID string) *Runner {
	return &Runner{
		Task:            newTask(taskID),
		process:         nil,
		processCallback: nil,
		runningFilter:   nil,
	}
}

// AddRunningFilter adds a filter to the runner, calling in the function all the current queued and running runners before running. If the function returns false, the runner will be queued again.
func (r *Runner) AddRunningFilter(f func(runnings []*Runner, currentRunner *Runner) bool) *Runner {
	if r.HasStarted() {
		log.Panic("Cannot add running filter to a runner that has already started")
	}
	r.runningFilter = f
	return r
}

// AddProcess adds a process to the runner. The process is a function that returns an error.
func (r *Runner) AddProcess(p func() error) *Runner {
	if r.HasStarted() {
		log.Panic("Cannot add process to a runner that has already started")
	}
	r.process = p
	return r
}

// AddProcessCallback adds a callback to the runner. The callback is a function that receives the engine and the runner as arguments. It is called after the process is executed, but not in a goroutine, so it will block the engine until it finishes.
func (r *Runner) AddProcessCallback(c func(engine *Engine, runner *Runner)) *Runner {
	if r.IsDone() {
		log.Panic("Cannot add process callback to a runner that is already done")
	}
	r.processCallback = c
	return r
}

// Run executes the runner process and calls the callback if it is set. (called in a goroutine by the engine)
func (r *Runner) Run(engine *Engine) error {
	if r.process != nil {
		engine.mu.Lock()
		if r.HasStarted() {
			engine.mu.Unlock()
			return errors.New(ERROR_RUNNER_ALREADY_STARTED)
		}
		r.Task.start()
		engine.mu.Unlock()
		if !r.MustInterrupt() {
			err := r.process()
			r.setError(err)
		}
		r.Task.end()
		return r.GetError()
	}
	log.Panicf("Runner %s has no process", r.ID)
	return nil
}

func (r *Runner) runCallbackIfRequired(engine *Engine) {
	if r.processCallback != nil && !r.MustInterrupt() {
		r.processCallback(engine, r)
	}
}
