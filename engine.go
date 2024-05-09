package gorunner

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Engine struct {
	Runners []*Runner
	wg      *sync.WaitGroup
	Done    map[string]int64
	mu      sync.RWMutex
	stop    bool
	options engineOptions
}

var ID int = 1

func NewEngine(options *engineOptions) *Engine {
	e := &Engine{
		wg:      &sync.WaitGroup{},
		Runners: []*Runner{},
		Done:    map[string]int64{},
		mu:      sync.RWMutex{},
		stop:    false,
		options: *options,
	}
	ID++
	return e
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
	t := engine.Done[runner.Task.ID]
	if t > 0 {
		if engine.options.removeFromHistoryIf(runner.Task.ID, time.Unix(t, 0)) {
			delete(engine.Done, runner.Task.ID)
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
	fmt.Printf("Engine status: Running: %d, Done: %d, Waiting: %d\n", countRunning, len(engine.Done), len(engine.Runners)-countRunning)
}

func (engine *Engine) handleRunnersDone() {
	for _, runner := range engine.Runners {
		task := runner.Task

		if task.IsDone() {
			if task.GetError() == nil {
				engine.mu.Lock()
				engine.Done[task.ID] = time.Now().Unix()
				engine.mu.Unlock()
				engine.Remove(runner)
			} else {
				log.Printf("Error running (%s) process: %s", task.ID, task.GetError().Error())
				newR := NewRunnerWithRetryCount(task.ID, task.RetryCount()+1)
				engine.Remove(runner)
				if runner.RetryCount() < engine.options.maxRetry {
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
	if engine.stop {
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
	//stop the sets
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
