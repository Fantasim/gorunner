package gorunner

import (
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

func newRunnerWithRetryCount(taskID string, retryCount int) *Runner {
	r := NewRunner(taskID)
	r.retry = retryCount
	return r
}

// AddRunningFilter adds a filter to the runner, calling in the function all the current queued and running runners before running. If the function returns false, the runner will be queued again.
func (r *Runner) AddRunningFilter(f func(runnings []*Runner, currentRunner *Runner) bool) *Runner {
	r.runningFilter = f
	return r
}

// AddProcess adds a process to the runner. The process is a function that returns an error.
func (r *Runner) AddProcess(p func() error) *Runner {
	r.process = p
	return r
}

// AddProcessCallback adds a callback to the runner. The callback is a function that receives the engine and the runner as arguments. It is called after the process is executed, but not in a goroutine, so it will block the engine until it finishes.
func (r *Runner) AddProcessCallback(c func(engine *Engine, runner *Runner)) *Runner {
	r.processCallback = c
	return r
}

// Run executes the runner process and calls the callback if it is set. (called in a goroutine by the engine)
func (r *Runner) Run() error {
	if r.process != nil {
		if r.HasStarted() || r.MustInterrupt() {
			return nil
		}
		r.Task.start()
		err := r.process()
		r.setError(err)
		r.Task.end()
		return err
	}
	log.Panicf("Runner %s has no process", r.ID)
	return nil
}

func (r *Runner) runCallbackIfRequired(engine *Engine) {
	if r.processCallback != nil && !r.MustInterrupt() {
		r.processCallback(engine, r)
	}
}
