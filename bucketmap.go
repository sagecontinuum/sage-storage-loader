package main

import "github.com/sagecontinuum/sage-storage-loader/rwmutex"

type BucketMap struct {
	rwmutex.RWMutex
	Map map[string]string // name(<nodeid>-<namespace>-<pluginname>-<version>-<YYYYMMDD>) -> id
	//rwmutex rwmutex.RWMutex
}

func (bm *BucketMap) Init(name string) {
	bm.Map = make(map[string]string)
	bm.RWMutex.Init(name)
}
