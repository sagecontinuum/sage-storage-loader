package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Worker struct {
	DataRoot             string
	Skipped              int64
	Uploader             FileUploader
	DeleteFilesOnSuccess bool
	Jobs                 <-chan Job
	Results              chan<- string
	Stop                 <-chan struct{}
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
	for job := range worker.Jobs {
		var result string

		if err := worker.Process(job); err != nil {
			result = fmt.Sprintf("error %s %s", job, err.Error())
		} else {
			result = fmt.Sprintf("ok %s", job)
		}

		select {
		case worker.Results <- result:
		case <-worker.Stop:
			return
		}
	}
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
	full_dir := filepath.Join(w.DataRoot, dir)

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
	doneFileLocal := filepath.Join(full_dir, DoneFilename)

	s3path := fmt.Sprintf("node-data/%s/sage-%s-%s/%s", p.Namespace, p.Name, p.Version, p.NodeID)

	uploadMeta := convertMetaToS3Meta(meta)

	if err := w.Uploader.UploadFile(dataFileLocal, filepath.Join(s3path, targetNameData), uploadMeta); err != nil {
		return err
	}

	if err := w.Uploader.UploadFile(metaFileLocal, filepath.Join(s3path, targetNameMeta), nil); err != nil {
		return err
	}

	if err := os.WriteFile(doneFileLocal, []byte{}, 0o644); err != nil {
		return fmt.Errorf("could not create flag file: %s", err.Error())
	}

	if w.DeleteFilesOnSuccess {
		// clean up data, meta, done files and then parent dir
		for _, name := range []string{dataFileLocal, metaFileLocal, doneFileLocal, filepath.Dir(dataFileLocal)} {
			if err := os.Remove(name); err != nil {
				return fmt.Errorf("failed to clean up %s", name)
			}
		}
	}

	// we don't want to remove parents yet... we'll clean those up later based on age...

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
