package gorunner

import (
	"math/rand"
	"strconv"
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

// func TestRunnerProcessErrorHandling(t *testing.T) {
// 	options := NewEngineOptions().SetMaxSimultaneousRunner(1).SetMaxRetry(1).SetRetryInterval(50 * time.Millisecond)
// 	engine := NewEngine(options)

// 	runner := NewRunner("errorRunner")
// 	processAttempts := 0
// 	mu := sync.Mutex{}

// 	runner.AddProcess(func() error {
// 		mu.Lock()
// 		defer mu.Unlock()
// 		processAttempts++
// 		return errors.New("process error")
// 	})

// 	engine.Add(runner)

// 	go engine.Execute()

// 	time.Sleep(300 * time.Millisecond)

// 	mu.Lock()
// 	assert.Equal(t, 2, processAttempts, "Runner should have retried once after initial error")
// 	mu.Unlock()
// }

// func TestEngineSimultaneousReadWrite(t *testing.T) {
// 	options := NewEngineOptions().SetMaxSimultaneousRunner(10)
// 	engine := NewEngine(options)

// 	var wg sync.WaitGroup

// 	for i := 0; i < 50; i++ {
// 		runner := NewRunner(string(i))
// 		runner.AddProcess(func() error {
// 			time.Sleep(50 * time.Millisecond)
// 			return nil
// 		})

// 		wg.Add(1)
// 		go func(r *Runner) {
// 			defer wg.Done()
// 			engine.Add(r)
// 		}(runner)
// 	}

// 	// Start the engine execution in a separate goroutine
// 	go engine.Execute()

// 	// Concurrently read engine status
// 	for i := 0; i < 50; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			engine.PrintStatus()
// 		}()
// 	}

// 	// Wait for all operations to complete
// 	wg.Wait()
// }
