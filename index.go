package main

import (
	"errors"
	"log"
	"time"

	"github.com/sagecontinuum/sage-storage-loader/rwmutex"
)

var ErrNotFound = errors.New("not found")

var timeout = time.Second * 2

type Index struct {
	rwmutex.RWMutex
	Map map[string]State
	//rwmutex rwmutex.RWMutex
}

func (index *Index) Init(name string) {
	index.Map = make(map[string]State)
	index.RWMutex.Init(name)
}

func (index *Index) Get(path string) (state State, err error) {
	rlock, err := index.RLockNamedTimeout("get", timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer index.RUnlockNamed(rlock)
	state, ok := index.Map[path]
	if !ok {
		err = ErrNotFound
		return
	}

	return
}

func (index *Index) GetList(state State) (resultList []string, err error) {
	rlock, err := index.RLockNamedTimeout("GetList", timeout)
	if err != nil {
		log.Fatal(err)
	}
	defer index.RUnlockNamed(rlock)

	for k, v := range index.Map {
		if v == state {
			resultList = append(resultList, k)
		}

	}

	return
}

// Add will return if key already exists
func (index *Index) Add(path string) (added bool, err error) {
	added = false

	err = index.LockNamedTimeout("add-write", timeout)
	defer index.Unlock()
	if err != nil {
		log.Fatal(err)
	}

	_, ok := index.Map[path]
	if ok {
		// silent return
		return
	}

	//job := &Job{dir: path, state: Pending}

	index.Map[path] = Pending
	added = true
	return
}

// Set will set state without checking
func (index *Index) Set(path string, state State, caller string) (err error) {

	err = index.LockNamedTimeout(caller, timeout)
	defer index.Unlock()
	if err != nil {
		log.Fatal(err)
	}

	index.Map[path] = state
	return
}

func (index *Index) Remove(path string, caller string) (err error) {

	err = index.LockNamedTimeout(caller, timeout)
	defer index.Unlock()
	if err != nil {
		log.Fatal(err)
	}

	delete(index.Map, path)

	return
}

// for debugging only (no locks used)
func (index *Index) Print() {
	for key, value := range index.Map {
		log.Printf("key: %s %s", key, value.String())
	}
}
