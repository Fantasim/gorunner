package gorunner

import (
	"errors"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	// Adjust the import path according to your project structure

	"github.com/stretchr/testify/assert"
)

func TestRunnerNotRunTwice(t *testing.T) {
	options := NewEngineOptions().SetMaxSimultaneousRunner(12)
	engine := NewEngine(options)

	size := 1_000

	runCount := atomic.Int64{}
	for i := 0; i < size; i++ {
		go func(i int, engine *Engine) {
			id := strconv.Itoa(i) + "_tasktest"
			runner := NewRunner(id)
			runner.AddProcess(func() error {
				n := rand.Intn(10) + 1
				time.Sleep(time.Duration(n) * time.Microsecond) // Simulate some work
				runCount.Add(1)
				return nil
			})
			runner.AddProcessCallback(func(engine *Engine, runner *Runner) {
				runCount.Add(1)
			})

			engine.Add(runner)
		}(i, engine)
	}

	engine.WaitForRunningTasks(nil)
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(size)*2, runCount.Load(), "Runner should not run more than once")
	assert.Equal(t, size, engine.CountDone(), "Runner should be done")
}

func TestRunnerProcessErrorHandling(t *testing.T) {
	options := NewEngineOptions().SetMaxSimultaneousRunner(1).SetMaxRetry(1).SetRetryInterval(50 * time.Millisecond)
	engine := NewEngine(options)

	runner := NewRunner("errorRunner")
	runCount := atomic.Int64{}

	runner.AddProcess(func() error {
		runCount.Add(1)
		return errors.New("process error")
	})

	engine.Add(runner)

	time.Sleep(300 * time.Millisecond)

	engine.WaitForRunningTasks(nil)
	assert.Equal(t, int64(2), runCount.Load(), "Runner should have retried once after initial error")
}

func TestFilter(t *testing.T) {
	options := NewEngineOptions().SetMaxSimultaneousRunner(10).SetRetryInterval(10 * time.Millisecond)
	engine := NewEngine(options)

	ret := ""
	mu := sync.Mutex{}

	start := time.Now()

	a := NewRunner("a")
	b := NewRunner("b")
	c := NewRunner("c")

	wg := sync.WaitGroup{}
	wg.Add(3)

	addProcess := func(r *Runner) {
		r.AddProcess(func() error {
			mu.Lock()
			ret += r.ID
			mu.Unlock()
			return nil
		})
		r.AddProcessCallback(func(engine *Engine, runner *Runner) {
			wg.Done()
		})
	}

	addProcess(a)
	addProcess(b)
	addProcess(c)

	a.AddRunningFilter(func(details EngineDetails, runner *Runner) bool {
		_, ok := details.Done["b"]
		return ok
	})
	b.AddRunningFilter(func(details EngineDetails, runner *Runner) bool {
		_, ok := details.Done["c"]
		return ok
	})
	c.AddRunningFilter(func(details EngineDetails, runner *Runner) bool {
		return time.Since(start) > 500*time.Millisecond
	})

	go engine.Add(a)
	go engine.Add(b)
	go engine.Add(c)

	wg.Wait()
	tt := engine.done["c"]

	time.Sleep(time.Millisecond * 5)
	assert.Equal(t, "cba", ret, "Runner should run in order")
	assert.Greater(t, tt.Sub(start), 500*time.Millisecond, "Runner c should run after 500ms")
}
