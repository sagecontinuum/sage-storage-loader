package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Job string

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
	Name         string            `json:"name"` // always set as "upload"
	EpochNano    int64             `json:"ts,omitempty"`
	EpochNanoOld *int64            `json:"timestamp,omitempty"` // only for reading (deprecated soon), keep it backwards compatible
	Shasum       *string           `json:"shasum,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"` // only read (will write to meta)
	Meta         map[string]string `json:"meta"`
	Value        string            `json:"val"` // only used to output URL, input is assumed to be empty
}

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

type UploadInfo struct {
	NodeID    string
	Namespace string
	Name      string
	Version   string
}

func parseUploadPath(dir string) (*UploadInfo, error) {
	dir_array := strings.Split(dir, "/")

	p := &UploadInfo{
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

func (w *Worker) Process(job Job) error {
	dir := string(job) // starts with  node-000048b02d...
	uploadDir := filepath.Join(w.DataRoot, dir)

	p, err := parseUploadPath(dir)
	if err != nil {
		return err
	}

	var meta MetaData

	if err := readMetaFile(filepath.Join(uploadDir, "meta"), &meta); err != nil {
		return err
	}

	// Add info extracted from path.
	meta.Meta["node"] = strings.ToLower(p.NodeID)
	// TODO(sean) reconcile namespace and job usage
	meta.Meta["plugin"] = p.Namespace + "/" + p.Name + ":" + p.Version

	labelFilename := meta.Meta["filename"]
	targetNameData := fmt.Sprintf("%d-%s", meta.EpochNano, labelFilename)
	targetNameMeta := fmt.Sprintf("%d-%s.meta", meta.EpochNano, labelFilename)

	dataFileLocal := filepath.Join(uploadDir, "data")
	metaFileLocal := filepath.Join(uploadDir, "meta")
	doneFileLocal := filepath.Join(uploadDir, DoneFilename)

	s3path := fmt.Sprintf("node-data/%s/sage-%s-%s/%s", p.Namespace, p.Name, p.Version, p.NodeID)

	uploadMeta := convertMetaToS3Meta(&meta)

	if err := w.Uploader.UploadFile(dataFileLocal, filepath.Join(s3path, targetNameData), uploadMeta); err != nil {
		return err
	}

	if err := w.Uploader.UploadFile(metaFileLocal, filepath.Join(s3path, targetNameMeta), nil); err != nil {
		return err
	}

	if err := os.WriteFile(doneFileLocal, []byte{}, 0o644); err != nil {
		return fmt.Errorf("could not create flag file: %s", err.Error())
	}

	// TODO(sean) If we see the need to support various clean up strategies,
	// we should just make this step plugable. For example, maybe instead of
	// deleting, we want to move files to a done directory.
	if w.DeleteFilesOnSuccess {
		// Clean up data, meta and done files.
		for _, name := range []string{dataFileLocal, metaFileLocal, doneFileLocal} {
			if err := os.Remove(name); err != nil {
				return fmt.Errorf("failed to clean up %s", name)
			}
		}

		// Attempt to clean up parent directories up to root/node-xyz/uploads.
		//
		// NOTE(sean) There is a possible race condition with the upload agent here.
		//
		// It's possible that the upload agent creates the parent paths which are removed
		// before we can upload. In this case, that particular rsync will fail and then
		// will be tried again later.
		//
		// In order for this to happen, the OSN loader would have to upload and clean up the
		// last staged item for a task right when that task is posting a new upload. This seems
		// potentially rare enough that I'd opt for simpler, more robust cleanup logic for now.
		for p := filepath.Dir(dataFileLocal); filepath.Base(p) != "uploads"; p = filepath.Dir(p) {
			if err := os.Remove(p); err != nil {
				break
			}
		}
	}

	return nil
}

func convertMetaToS3Meta(meta *MetaData) map[string]string {
	m := map[string]string{
		"name": meta.Name,
		"ts":   strconv.FormatInt(meta.EpochNano, 10),
	}
	if meta.Shasum != nil {
		m["shasum"] = *meta.Shasum
	}
	for k, v := range meta.Meta {
		m["meta."+k] = v
	}
	return m
}

func readMetaFile(name string, m *MetaData) error {
	if err := readJSONFile(name, m); err != nil {
		return err
	}

	m.Name = "upload"

	if m.EpochNanoOld != nil {
		m.EpochNano = *m.EpochNanoOld
		m.EpochNanoOld = nil
	}

	// read Labels only if Meta is empty
	if m.Meta == nil {
		m.Meta = m.Labels
		m.Labels = nil
	}

	m.Shasum = nil

	filename, ok := m.Meta["filename"]
	if !ok {
		return fmt.Errorf("missing filename metadata")
	}
	if filename == "" {
		return fmt.Errorf("filename metadata cannot be empty")
	}

	return nil
}

func readJSONFile(name string, v interface{}) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(v)
}
