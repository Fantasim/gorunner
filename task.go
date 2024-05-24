package gorunner

import (
	"log"
	"sync/atomic"
	"time"
)

const currentTaskSizeKey = "CURRENT_TASK_SIZE__"
const maxTaskSizeKey = "MAX_TASK_SIZE__"
const initialTaskSizeKey = "INITIAL_TASK_SIZE__"

type Task struct {
	//monitoring
	ID        string
	startedAt time.Time
	endedAt   time.Time

	//logging
	steps      []time.Time
	statValues map[string]*atomic.Int64

	//args
	Args map[string]interface{}

	quit bool
	//errors
	retry         int
	retryDisabled bool
	err           error
}

func (t *Task) AddArgs(key string, v interface{}) {
	if t.HasStarted() {
		log.Panic("Cannot add args to a task that has already started")
	}
	if t.Args == nil {
		t.Args = map[string]interface{}{}
	}
	t.Args[key] = v
}

func newTask(ID string) *Task {
	t := &Task{
		ID:            ID,
		err:           nil,
		steps:         []time.Time{},
		statValues:    map[string]*atomic.Int64{},
		Args:          map[string]interface{}{},
		quit:          false,
		retry:         0,
		retryDisabled: false,
	}
	t.SetStatValue(initialTaskSizeKey, 0)
	t.SetStatValue(currentTaskSizeKey, 0)
	t.SetStatValue(maxTaskSizeKey, 0)
	return t
}

func (task *Task) SetRetryCount(count int) {
	task.retry = count
}

// SizePerMillisecond returns the size added on average per millisecond since the task started
func (task *Task) SizePerMillisecond() float64 {
	currentSize := task.Size().Current()
	if currentSize == 0 {
		return 0
	}
	return float64(currentSize-task.Size().Initial()) / float64(time.Since(task.StartedAt()).Milliseconds())
}

// Timer returns the time elapsed since the task started
func (task *Task) Timer() time.Duration {
	if !task.HasStarted() {
		return 0
	}
	if !task.IsDone() {
		return time.Since(task.StartedAt())
	}
	return task.EndedAt().Sub(task.StartedAt())
}

// Percent returns the percentage of the task that has been accomplished only if the task has sizes set
func (task *Task) Percent() float64 {
	max := task.Size().Max()
	if !task.HasStarted() || max <= 0 {
		return 0
	}
	if task.IsDone() {
		return 100
	}
	initial := task.Size().Initial()
	return (float64(task.Size().Current()-initial) / float64(max-initial)) * 100
}

// ETA returns the estimated time of arrival of the task only if the task has sizes set
func (task *Task) ETA() time.Duration {
	percent := task.Percent()
	if percent > 0 && percent < 100 {
		eta := time.Duration(float64(task.Timer()) / percent * (100 - percent))
		return eta
	}
	return 0
}

type sizeGetter struct {
	Initial func() int64
	Current func() int64
	Max     func() int64
}

func (task *Task) Size() sizeGetter {
	return sizeGetter{
		Initial: func() int64 {
			return task.StatValue(initialTaskSizeKey)
		},
		Current: func() int64 {
			return task.StatValue(currentTaskSizeKey)
		},
		Max: func() int64 {
			return task.StatValue(maxTaskSizeKey)
		},
	}
}

type sizeSetter struct {
	Initial func(size int64)
	Current func(size int64)
	Max     func(size int64)
}

func (task *Task) SetSize() sizeSetter {
	return sizeSetter{
		Initial: func(size int64) {
			if task.IsDone() {
				return
			}
			if size > 0 {
				if task.Size().Initial() > 0 {
					log.Panic("Initial task size cannot be set more than once:", task.ID)
				}
				task.SetStatValue(initialTaskSizeKey, size)
				if task.Size().Current() == 0 {
					task.SetSize().Current(size)
				}
			}
		},
		Current: func(size int64) {
			if task.IsDone() {
				return
			}
			if size > 0 {
				initial := task.Size().Initial()
				if initial > size {
					log.Panic("Current task size cannot be less than the initial task size:", task.ID)
				}
				max := task.Size().Max()
				if max > 0 && size > max {
					size = max
				}
				task.SetStatValue(currentTaskSizeKey, size)
			}
		},
		Max: func(size int64) {
			if task.IsDone() {
				return
			}

			if size > 0 {
				if task.Size().Max() > 0 {
					log.Panic("Max task size cannot be set more than once:", task.ID)
				}
				task.SetStatValue(maxTaskSizeKey, size)
			}
		},
	}
}

func (task *Task) Interrupt() {
	task.quit = true
}

func (task *Task) MustInterrupt() bool {
	return task.quit
}

func (task *Task) AreArgsEqual(args map[string]interface{}) bool {
	for key := range args {
		if task.Args[key] != args[key] {
			return false
		}
	}
	return true
}

func (task *Task) RetryCount() int {
	return task.retry
}

func (task *Task) AddStep() {
	task.steps = append(task.steps, time.Now())
}

func (task *Task) StatValue(key string) int64 {
	v, ok := task.statValues[key]
	if ok {
		return v.Add(0)
	}
	return 0
}

func (task *Task) SetStatValue(key string, value int64) {
	_, ok := task.statValues[key]
	if ok {
		task.statValues[key].Store(value)
	} else {
		task.statValues[key] = &atomic.Int64{}
		task.statValues[key].Store(value)
	}
}

func (task *Task) IncrementStatValue(key string, value int64) {
	if _, ok := task.statValues[key]; !ok {
		task.SetStatValue(key, value)
	}
	task.statValues[key].Add(value)
}

func (task *Task) start() {
	task.startedAt = time.Now()
}

func (task *Task) end() {
	task.endedAt = time.Now()
}

func (task *Task) HasStarted() bool {
	return !task.startedAt.IsZero()
}

func (task *Task) StartedAt() time.Time {
	return task.startedAt
}

func (task *Task) EndedAt() time.Time {
	return task.endedAt
}

func (task *Task) setError(err error) {
	task.err = err
}

func (task *Task) GetError() error {
	return task.err
}

func (task *Task) IsDone() bool {
	return !task.startedAt.IsZero() && !task.endedAt.IsZero()
}

func (task *Task) IsRunning() bool {
	return !task.startedAt.IsZero() && task.endedAt.IsZero()
}

func (task *Task) CountSteps() int {
	return len(task.steps)
}

func (task *Task) LastStep() time.Time {
	if len(task.steps) == 0 {
		return time.Time{}
	}
	return task.steps[len(task.steps)-1]
}

func (t *Task) DisableRetry() {
	t.retryDisabled = true
}

func GetArg[T any](args map[string]interface{}, key string) (T, bool) {
	if val, ok := args[key]; ok {
		typedVal, ok := val.(T)
		return typedVal, ok
	}
	var zeroVal T // Create a zero value of type T
	return zeroVal, false
}
