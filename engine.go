package gorunner

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Engine struct {
	Runners     []*Runner
	wg          *sync.WaitGroup
	done        map[string]int64
	mu          sync.RWMutex
	stop        bool
	options     engineOptions
	pausedUntil time.Time
}

var ID int = 1

func NewEngine(options *engineOptions) *Engine {
	e := &Engine{
		wg:          &sync.WaitGroup{},
		Runners:     []*Runner{},
		done:        map[string]int64{},
		mu:          sync.RWMutex{},
		stop:        false,
		options:     *options,
		pausedUntil: time.Time{},
	}
	ID++
	return e
}

func (e *Engine) Pause(forDuration time.Duration) {
	if forDuration <= 0 {
		e.Execute()
		return
	}

	e.pausedUntil = time.Now().Add(forDuration)
	//call after end of the duration
	go func() {
		time.Sleep(forDuration)
		e.pausedUntil = time.Time{}
		e.Execute()
	}()
}

func (e *Engine) WaitForRunningTasks() {
	e.wg.Wait()
}

func (engine *Engine) CountRunning() int {
	count := 0
	for _, runner := range engine.Runners {
		if runner.IsRunning() {
			count++
		}
	}
	return count
}

func (engine *Engine) CountDone() int {
	return len(engine.done)
}

func (engine *Engine) CountQueued() int {
	return len(engine.Runners) - engine.CountRunning()
}

func (engine *Engine) IsTaskDone(taskID string) bool {
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	_, ok := engine.done[taskID]
	return ok
}

func (engine *Engine) Remove(r *Runner) {
	engine.mu.Lock()
	defer engine.mu.Unlock()
	for i, p := range engine.Runners {
		if p == r {
			engine.Runners = append(engine.Runners[:i], engine.Runners[i+1:]...)
			break
		}
	}
}

func (engine *Engine) Add(runner *Runner) {
	if engine.stop {
		return
	}

	engine.mu.Lock()
	t := engine.done[runner.Task.ID]
	if t > 0 {
		if engine.options.removeFromHistoryIf(runner.Task.ID, time.Unix(t, 0)) {
			delete(engine.done, runner.Task.ID)
		} else {
			engine.mu.Unlock()
			return
		}
	}
	for _, p := range engine.Runners {
		if p.Task.ID == runner.Task.ID {
			engine.mu.Unlock()
			return
		}
	}

	engine.Runners = append(engine.Runners, runner)
	engine.mu.Unlock()
	engine.Execute()
}

func (engine *Engine) PrintStatus() {
	countRunning := engine.CountRunning()
	fmt.Printf("Engine status: Running: %d, Done: %d, Waiting: %d\n", countRunning, len(engine.done), len(engine.Runners)-countRunning)
}

func (engine *Engine) handleRunnersDone() {
	for _, runner := range engine.Runners {
		task := runner.Task

		if task.IsDone() {
			if task.GetError() == nil {
				engine.mu.Lock()
				engine.done[task.ID] = time.Now().Unix()
				engine.mu.Unlock()
				engine.Remove(runner)
			} else {
				log.Printf("Error running (%s) process: %s", task.ID, task.GetError().Error())
				newR := NewRunnerWithRetryCount(task.ID, task.RetryCount()+1)
				engine.Remove(runner)
				if runner.RetryCount() < engine.options.maxRetry && !runner.retryDisabled {
					go func(r *Runner) {
						time.Sleep(engine.options.retryInterval)
						engine.Add(r)
					}(newR)
				}
			}
		}
	}
}

func (engine *Engine) Execute() {
	if engine.stop || engine.pausedUntil.After(time.Now()) {
		return
	}

	countRunning := engine.CountRunning()
	if countRunning >= engine.options.maxSimultaneousRunner {
		return
	}

	engine.handleRunnersDone()

	for _, runner := range engine.Runners {
		if !runner.Task.IsDone() && !runner.Task.IsRunning() {
			engine.wg.Add(1)
			go func(runner *Runner, engine *Engine) {
				runner.Run()
				engine.wg.Done()
				engine.Execute()
			}(runner, engine)
			break
		}
	}
}

func (engine *Engine) CountRunningByArgs(args map[string]interface{}) int {
	count := 0
	for _, runner := range engine.Runners {
		if runner.Task.IsRunning() && runner.Task.AreArgsEqual(args) {
			count++
		}
	}
	return count
}

func (engine *Engine) StopRunnersByArgs(args map[string]interface{}) {
	newList := []*Runner{}
	engine.mu.Lock()
	for _, runner := range engine.Runners {
		if !runner.Task.AreArgsEqual(args) || runner.Task.IsRunning() {
			newList = append(newList, runner)
		}
	}

	engine.Runners = newList
	engine.mu.Unlock()
	for {
		cnt := engine.CountRunningByArgs(args)
		if cnt == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
}

func (engine *Engine) StopAll() {
	engine.stop = true
	go func() {
		for {
			log.Printf("Engine %s is waiting for %d tasks to finish", engine.options.name, engine.CountRunning())
			time.Sleep(4 * time.Second)
		}
	}()
	for _, runner := range engine.Runners {
		if !runner.Task.IsRunning() {
			engine.Remove(runner)
		} else {
			runner.Interrupt()
		}
	}
	engine.WaitForRunningTasks()
	engine = NewEngine(&engine.options)
}
