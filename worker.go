package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Worker struct {
	ID                   int
	Skipped              int64
	Uploader             FileUploader
	DeleteFilesOnSuccess bool
	jobQueue             <-chan Job
	shutdown             <-chan struct{}
}

type MetaData struct {
	Name         string                 `json:"name"` // always set as "upload"
	EpochNano    int64                  `json:"ts,omitempty"`
	EpochNanoOld *int64                 `json:"timestamp,omitempty"` // only for reading (deprecated soon), keep it backwards compatible
	Shasum       *string                `json:"shasum,omitempty"`
	Labels       map[string]interface{} `json:"labels,omitempty"` // only read (will write to meta)
	Meta         map[string]interface{} `json:"meta"`
	Value        string                 `json:"val"` // only used to output URL, input is assumed to be empty
}

var sage_storage_api = "http://host.docker.internal:8080"
var sage_storage_token = "user:test"
var sage_storage_username = "test"

func (worker *Worker) Run() {
	fmt.Printf("Worker %d starting\n", worker.ID)

FOR:
	for {
		// by splitting this into two selects, is is guaranteed that the broadcast is not skipped
		select {
		case <-worker.shutdown:
			fmt.Printf("Worker %d received shutdown signal.\n", worker.ID)
			break FOR
		default:
		}

		select {
		case job := <-worker.jobQueue:
			err := worker.Process(job)
			if err != nil {
				log.Printf("Somthing went wrong: %s", err.Error())
				index.Set(string(job), Failed, "worker")
				err = nil
			} else {
				index.Set(string(job), Done, "worker")
			}
		default:
			// there is no work, slow down..
			time.Sleep(time.Second)
		}
	}
	fmt.Printf("Worker %d stopping.\n", worker.ID)
}

type pInfo struct {
	NodeID    string
	Namespace string
	Name      string
	Version   string
}

func parseUploadPath(dir string) (*pInfo, error) {
	dir_array := strings.Split(dir, "/")

	p := &pInfo{
		NodeID:    strings.TrimPrefix(dir_array[0], "node-"),
		Namespace: "sage", // sage is the default in cases where no namespace was given
		Name:      "",
		Version:   "",
	}

	switch len(dir_array) {
	case 6:
		p.Namespace = dir_array[2]
		p.Name = dir_array[3]
		p.Version = dir_array[4]
	case 5: // namespace is missing
		p.Name = dir_array[2]
		p.Version = dir_array[3]
	default:
		return nil, fmt.Errorf("could not parse path %s", dir)
	}

	return p, nil
}

func getMetadata(full_dir string) (*MetaData, error) {
	content, err := ioutil.ReadFile(filepath.Join(full_dir, "meta"))
	if err != nil {
		return nil, err
	}
	meta := &MetaData{}
	if err := json.Unmarshal(content, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (w *Worker) Process(job Job) error {
	dir := string(job) // starts with  node-000048b02d...
	full_dir := filepath.Join(dataDirectory, dir)

	p, err := parseUploadPath(dir)
	if err != nil {
		return err
	}

	meta, err := getMetadata(full_dir)
	if err != nil {
		return err
	}

	meta.Name = "upload"

	if meta.EpochNanoOld != nil {
		meta.EpochNano = *meta.EpochNanoOld
		meta.EpochNanoOld = nil
	}

	if meta.Meta == nil { // read Labels only if Meta is empty
		meta.Meta = meta.Labels
		meta.Labels = nil
	}
	meta.Shasum = nil

	labelFilenameIf, ok := meta.Meta["filename"]
	if !ok {
		return fmt.Errorf("label field  filename is missing")
	}
	labelFilename, ok := labelFilenameIf.(string)
	if !ok {
		return fmt.Errorf("label field filename is not a string")
	}
	if len(labelFilename) == 0 {
		return fmt.Errorf("label field filename is empty")
	}

	// add info extracted from path
	meta.Meta["node"] = strings.ToLower(p.NodeID)
	meta.Meta["plugin"] = p.Namespace + "/" + p.Name + ":" + p.Version

	targetNameData := fmt.Sprintf("%d-%s", meta.EpochNano, labelFilename)
	targetNameMeta := fmt.Sprintf("%d-%s.meta", meta.EpochNano, labelFilename)

	dataFileLocal := filepath.Join(full_dir, "data")
	metaFileLocal := filepath.Join(full_dir, "meta")

	rootFolder := "node-data"
	s3path := fmt.Sprintf("%s/%s/sage-%s-%s/%s", rootFolder, p.Namespace, p.Name, p.Version, p.NodeID)

	uploadMeta := convertMetaToS3Meta(meta)

	if err := w.Uploader.UploadFile(dataFileLocal, filepath.Join(s3path, targetNameData), uploadMeta); err != nil {
		return err
	}

	if err := w.Uploader.UploadFile(metaFileLocal, filepath.Join(s3path, targetNameMeta), nil); err != nil {
		return err
	}

	// *** delete files
	if w.DeleteFilesOnSuccess {
		if err := os.RemoveAll(full_dir); err != nil {
			return fmt.Errorf("can not delete directory (%s): %s", full_dir, err.Error())
		}
	} else {
		flag_file := filepath.Join(full_dir, "done")

		var emptyFlagFile *os.File
		emptyFlagFile, err = os.Create(flag_file)
		if err != nil {
			return fmt.Errorf("could not create flag file: %s", err.Error())
		}
		emptyFlagFile.Close()
	}

	return nil
}

func convertMetaToS3Meta(meta *MetaData) map[string]string {
	s3metadata := make(map[string]string)

	s3metadata["name"] = meta.Name
	s3metadata["ts"] = strconv.FormatInt(meta.EpochNano, 10)
	if meta.Shasum != nil {
		s3metadata["shasum"] = *meta.Shasum
	}

	for key, value := range meta.Meta {
		if s, ok := value.(string); ok {
			s3metadata["meta."+key] = s
		}
	}

	return s3metadata
}
