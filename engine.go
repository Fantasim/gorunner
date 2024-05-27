package gorunner

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Engine struct {
	allRunners     []*Runner
	runningRunners map[string]*Runner
	done           map[string]time.Time

	mu          sync.RWMutex
	stopCh      chan struct{}
	options     engineOptions
	pausedUntil time.Time
}

func NewEngine(options *engineOptions) *Engine {
	return &Engine{
		allRunners:     []*Runner{},
		done:           make(map[string]time.Time),
		stopCh:         make(chan struct{}),
		options:        *options,
		pausedUntil:    time.Time{},
		runningRunners: make(map[string]*Runner),
	}
}

func (e *Engine) Pause(forDuration time.Duration) {
	if forDuration <= 0 {
		e.Execute()
		return
	}

	e.pausedUntil = time.Now().Add(forDuration)
	go func() {
		time.Sleep(forDuration)
		e.pausedUntil = time.Time{}
		e.Execute()
	}()
}

func (e *Engine) WaitForRunningTasks(args map[string]interface{}) {
	for {
		if e.CountRunningByArgs(args) == 0 {
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func (e *Engine) CountRunning() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.runningRunners)
}

func (e *Engine) CountDone() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.done)
}

func (e *Engine) CountQueued() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.allRunners) - len(e.runningRunners)
}

func (e *Engine) IsTaskDone(taskID string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, ok := e.done[taskID]
	return ok
}

func (e *Engine) Cancel(r *Runner) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i, p := range e.allRunners {
		if p == r {
			if r.IsRunning() {
				r.interrupt()
			}
			e.allRunners = append(e.allRunners[:i], e.allRunners[i+1:]...)
			break
		}
	}
}

func (e *Engine) unsafeCancel(r *Runner) {
	for i, p := range e.allRunners {
		if p == r {
			e.allRunners = append(e.allRunners[:i], e.allRunners[i+1:]...)
			break
		}
	}
}

func (e *Engine) Add(runner *Runner) {
	if e.stopCh == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if t, ok := e.done[runner.ID]; ok {
		if !e.options.shouldRunAgain(runner.ID, t) {
			return
		}
		delete(e.done, runner.ID)
	}

	for _, p := range e.allRunners {
		if p.ID == runner.ID {
			return
		}
	}

	e.allRunners = append(e.allRunners, runner)
	go e.Execute()
}

func (e *Engine) PrintStatus() {
	paused := "false"
	if e.pausedUntil.After(time.Now()) {
		paused = "true"
	}

	fmt.Printf("Engine status: Running: %d, Done: %d, Waiting: %d, Paused: %s\n", e.CountRunning(), len(e.done), e.CountQueued(), paused)
}

func (e *Engine) Execute() {
	if e.stopCh == nil || e.pausedUntil.After(time.Now()) {
		return
	}

	if e.CountRunning() >= e.options.maxSimultaneousRunner {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, runner := range e.allRunners {
		if !runner.IsDone() && !runner.IsRunning() {
			if runner.runningFilter != nil {
				details := EngineDetails{
					Done:           e.done,
					RunningRunners: e.runningRunners,
					AllRunners:     e.allRunners,
				}
				if !runner.runningFilter(details, runner) {
					time.AfterFunc(e.options.retryInterval, e.Execute)
					continue
				}
			}

			e.runningRunners[runner.ID] = runner
			go func(runner *Runner, engine *Engine) {
				err := runner.Run(engine)
				if err != nil && (err.Error() == ERROR_RUNNER_ALREADY_STARTED) {
					go engine.Execute()
					return
				}

				engine.mu.Lock()
				//remove runner from running runners map
				delete(engine.runningRunners, runner.ID)
				// remove runner from all runners (without mutex lock)
				engine.unsafeCancel(runner)

				if err == nil {
					//setting runner as done in done map
					engine.done[runner.ID] = time.Now()
					// run callback if required
					go runner.runCallbackIfRequired(e)

				} else {
					//logging
					log.Printf("Error running (%s) process: %s", runner.ID, runner.GetError().Error())

					//removing all running data values + incrementing retry count
					runner.setupForRetry()

					//if retry count is less than max retry count and retry is not disabled, add runner again
					if runner.RetryCount() <= e.options.maxRetry && !runner.retryDisabled {
						time.AfterFunc(engine.options.retryInterval, func() {
							engine.Add(runner)
						})
					}
				}
				engine.mu.Unlock()
				engine.Execute()
			}(runner, e)
			return
		}
	}
}

func (e *Engine) RunningRunners() []*Runner {
	e.mu.RLock()
	defer e.mu.RUnlock()

	runningRunners := make([]*Runner, 0, len(e.runningRunners))
	for _, runner := range e.runningRunners {
		runningRunners = append(runningRunners, runner)
	}
	return runningRunners
}

func (e *Engine) CancelRunnersByArgs(args map[string]interface{}) {
	e.mu.Lock()
	newList := []*Runner{}
	for _, runner := range e.allRunners {
		if !runner.AreArgsEqual(args) || runner.IsRunning() {
			newList = append(newList, runner)
		}
	}
	e.allRunners = newList
	e.mu.Unlock()

	e.WaitForRunningTasks(args)
}

func (e *Engine) CountRunningByArgs(args map[string]interface{}) int {
	e.mu.RLock()
	defer e.mu.RUnlock()

	count := 0
	for _, runner := range e.runningRunners {
		if runner.AreArgsEqual(args) {
			count++
		}
	}
	return count
}

func (e *Engine) Quit() {
	close(e.stopCh)
	e.mu.Lock()
	for _, runner := range e.allRunners {
		if runner.IsRunning() {
			runner.interrupt()
		}
	}
	e.mu.Unlock()
	e.WaitForRunningTasks(nil)

	e.allRunners = nil
	e.stopCh = nil
}
