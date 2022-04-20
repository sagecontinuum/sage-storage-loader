package main

type State int

const (
	Pending State = iota
	Active        // means enqueued and/or uploading
	Done          // indicate file has been uploaded and deleted
	Failed
)

var stateStrings = []string{
	Pending: "PENDING",
	Active:  "ACTIVE",
	Done:    "DONE",
	Failed:  "FAILED",
}

func (d State) String() string {
	return stateStrings[d]
}
