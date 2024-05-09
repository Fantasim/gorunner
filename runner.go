package gorunner

type Runner struct {
	*Task
	process func() error
}

func NewRunner(taskID string) *Runner {
	task := newTask(taskID)
	return &Runner{
		Task:    task,
		process: nil,
	}
}

func NewRunnerWithRetryCount(taskID string, retryCount int) *Runner {
	task := newTask(taskID)
	task.retry = retryCount

	return &Runner{
		Task:    task,
		process: nil,
	}
}

func (r *Runner) AddProcess(p func() error) error {
	r.process = p
	return nil
}

func (r *Runner) Run() error {
	if r.process != nil {
		if r.HasStarted() {
			return nil
		}
		r.Task.Start()
		defer r.Task.End()
		err := r.process()
		r.SetError(err)
		return err
	}
	return nil
}
