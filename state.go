package main

type State int

const (
	Pending State = iota
	Active        // means enqueued and/or uploading
	Done          // indicate file has been uploaded and deleted
	Failed
)

func (d State) String() string {
	return [...]string{"PENDING", "ACTIVE", "DONE", "FAILED"}[d]
}
