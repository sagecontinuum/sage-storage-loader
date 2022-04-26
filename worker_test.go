package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanAndProcess(t *testing.T) {
	testcases := map[string][]struct {
		path   string
		data   []byte
		upload string
	}{
		"NoJob": {
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(``),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"Job": {
			{
				path:   "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(``),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"SkipNoData": {
			{
				path: "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"SkipNoMeta": {
			{
				path: "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(``),
			},
		},
		"SkipDone": {
			{
				path: "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(``),
			},
			{
				path: "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/.done",
				data: []byte(``),
			},
		},
		"Combined": {
			{
				path:   "node-0000000000000001/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000001/1638576647406523064-wow1.txt",
				data:   []byte(``),
			},
			{
				path:   "node-0000000000000001/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000001/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path:   "node-0000000000000002/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000002/1638576647406523064-wow2.jpg",
				data:   []byte(``),
			},
			{
				path:   "node-0000000000000002/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000002/1638576647406523064-wow2.jpg.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow2.jpg"}}`),
			},
			{
				path:   "node-0000000000000003/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/0000000000000003/1638576647406523064-wow3.flac",
				data:   []byte(``),
			},
			{
				path:   "node-0000000000000003/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/0000000000000003/1638576647406523064-wow3.flac.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow3.flac"}}`),
			},
		},
	}

	for name, files := range testcases {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "data")

			for _, file := range files {
				path := filepath.Join(root, file.path)
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, file.data, 0o644); err != nil {
					t.Fatal(err)
				}
			}

			stop := make(chan struct{})
			defer close(stop)
			jobs := make(chan Job)

			go func() {
				scanForJobs(stop, jobs, root)
				close(jobs)
			}()

			uploader := NewMockUploader()
			worker := &Worker{Uploader: uploader}

			for job := range jobs {
				worker.Process(job)
			}

			// check that all expected uploads exist
			uploads := make(map[pair]bool)

			for _, file := range files {
				if file.upload != "" {
					uploads[pair{filepath.Join(root, file.path), file.upload}] = true
				}
			}

			for k := range uploads {
				if !uploader.Uploads[k] {
					t.Fatalf("missing upload\nsrc: %s\ndst: %s", k.src, k.dst)
				}
			}

			for k := range uploader.Uploads {
				if !uploads[k] {
					t.Fatalf("unexpected upload\nsrc: %s\ndst: %s", k.src, k.dst)
				}
			}
		})
	}
}

type pair struct{ src, dst string }

type MockUploader struct {
	Error   error
	Uploads map[pair]bool
}

func NewMockUploader() *MockUploader {
	return &MockUploader{
		Uploads: make(map[pair]bool),
	}
}

func (up *MockUploader) UploadFile(src, dst string, meta *MetaData) error {
	if up.Error != nil {
		return up.Error
	}
	up.Uploads[pair{src, dst}] = true
	return nil
}
