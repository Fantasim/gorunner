package gorunner

import (
	"sync/atomic"
	"time"
)

type Task struct {
	//monitoring
	ID        string
	startedAt time.Time
	endedAt   time.Time

	//logging
	Steps      []time.Time
	statValues map[string]*atomic.Int64

	//args
	Args map[string]interface{}

	//errors
	retry int
	err   error
}

func (t *Task) AddArgs(key string, v interface{}) {
	if t.Args == nil {
		t.Args = map[string]interface{}{}
	}
	t.Args[key] = v
}

func newTask(ID string) *Task {
	return &Task{
		ID:         ID,
		err:        nil,
		Steps:      []time.Time{},
		statValues: map[string]*atomic.Int64{},
		Args:       map[string]interface{}{},
	}
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
	task.Steps = append(task.Steps, time.Now())
}

func (task *Task) StatValue(key string) int64 {
	v, ok := task.statValues[key]
	if ok {
		return v.Add(0)
	}
	return 0
}

func (task *Task) IncrementStatValue(stat string, value int64) {
	if _, ok := task.statValues[stat]; !ok {
		task.statValues[stat] = &atomic.Int64{}
		task.statValues[stat].Store(value)
	}
	task.statValues[stat].Add(value)
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

func (task *Task) SetError(err error) {
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
	return len(task.Steps)
}

func (task *Task) LastStep() time.Time {
	if len(task.Steps) == 0 {
		return time.Time{}
	}
	return task.Steps[len(task.Steps)-1]
}

func GetArg[T any](args map[string]interface{}, key string) (T, bool) {
	if val, ok := args[key]; ok {
		typedVal, ok := val.(T)
		return typedVal, ok
	}
	var zeroVal T // Create a zero value of type T
	return zeroVal, false
}
