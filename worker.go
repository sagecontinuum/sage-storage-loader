package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
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
	Name      string
	Timestamp time.Time
	Shasum    *string
	Meta      map[string]string
	Value     string
}

type UploadInfo struct {
	NodeID    string
	Namespace string
	Name      string
	Version   string
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
	targetNameData := fmt.Sprintf("%d-%s", meta.Timestamp.UnixNano(), labelFilename)
	targetNameMeta := fmt.Sprintf("%d-%s.meta", meta.Timestamp.UnixNano(), labelFilename)

	dataFileLocal := filepath.Join(uploadDir, "data")
	metaFileLocal := filepath.Join(uploadDir, "meta")
	doneFileLocal := filepath.Join(uploadDir, DoneFilename)

	s3path := fmt.Sprintf("node-data/%s/sage-%s-%s/%s", p.Namespace, p.Name, p.Version, p.NodeID)

	if err := w.Uploader.UploadFile(dataFileLocal, filepath.Join(s3path, targetNameData), &meta); err != nil {
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

func readMetaFile(name string, m *MetaData) error {
	var data struct {
		EpochNano    *int64            `json:"ts"`
		EpochNanoOld *int64            `json:"timestamp"` // only for reading (deprecated soon), keep it backwards compatible
		Shasum       *string           `json:"shasum"`
		Meta         map[string]string `json:"meta"`
		MetaOld      map[string]string `json:"labels"` // only read (will write to meta)
	}

	if err := readJSONFile(name, &data); err != nil {
		return err
	}

	m.Name = "upload"

	// detect timestamp
	switch {
	case data.EpochNano != nil:
		m.Timestamp = time.Unix(0, *data.EpochNano)
	case data.EpochNanoOld != nil:
		m.Timestamp = time.Unix(0, *data.EpochNanoOld)
	default:
		return fmt.Errorf("meta file is missing timestamp")
	}

	// detect meta
	switch {
	case data.Meta != nil:
		m.Meta = data.Meta
	case data.MetaOld != nil:
		m.Meta = data.MetaOld
	default:
		return fmt.Errorf("meta file is missing meta fields")
	}

	if m.Meta["filename"] == "" {
		return fmt.Errorf("filename metadata must exist and be nonempty")
	}

	if data.Shasum != nil {
		m.Shasum = data.Shasum
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
