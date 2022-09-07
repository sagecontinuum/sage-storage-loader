package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

type testfile struct {
	path   string
	data   []byte
	upload string
}

func tempDirFromTestFiles(t *testing.T, testfiles []testfile) string {
	// generate temp directory and write out all test files
	root := filepath.Join(t.TempDir(), "data")

	for _, file := range testfiles {
		path := filepath.Join(root, file.path)

		// ensure parents exist
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}

		// write file
		if err := os.WriteFile(path, file.data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	return root
}

type pair struct{ src, dst string }

type mockUploader struct {
	Error   error
	Uploads map[pair]bool
}

func newMockUploader() *mockUploader {
	return &mockUploader{
		Uploads: make(map[pair]bool),
	}
}

func (up *mockUploader) UploadFile(src, dst string, meta *MetaData) error {
	if up.Error != nil {
		return up.Error
	}
	up.Uploads[pair{src, dst}] = true
	return nil
}

func scanAndProcessDirOnce(worker *Worker, root string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	jobs := make(chan Job)

	go func() {
		scanForJobs(ctx, jobs, root)
		close(jobs)
	}()

	for job := range jobs {
		worker.Process(job)
	}

	return nil
}

func TestScanAndProcess(t *testing.T) {
	testcases := map[string][]testfile{
		"DefaultSageJob": {
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(`some data`),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"DefaultSageJobLeadingV": {
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/v0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-v0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(`some data`),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/v0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-v0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"Job": {
			{
				path:   "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(`some more data`),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"DefaultLatest": {
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/latest/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-latest/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(`some more data`),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/imagesampler-top/latest/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-latest/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"Latest": {
			{
				path:   "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/latest/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/Pluginctl/sage-imagesampler-top-latest/000048b02d15bc7c/1638576647406523064-wow1.txt",
				data:   []byte(`some more data`),
			},
			{
				path:   "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/latest/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/Pluginctl/sage-imagesampler-top-latest/000048b02d15bc7c/1638576647406523064-wow1.txt.meta",
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
				data: []byte(`where's my meta file?`),
			},
		},
		"SkipDone": {
			{
				path: "node-000048b02d15bc7c/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(`we already uploaded this`),
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
				data:   []byte(`all of these`),
			},
			{
				path:   "node-0000000000000001/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000001/1638576647406523064-wow1.txt.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path:   "node-0000000000000002/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000002/1638576647406523064-wow2.jpg",
				data:   []byte(`will be`),
			},
			{
				path:   "node-0000000000000002/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/sage/sage-imagesampler-top-0.2.5/0000000000000002/1638576647406523064-wow2.jpg.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow2.jpg"}}`),
			},
			{
				path:   "node-0000000000000003/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/0000000000000003/1638576647406523064-wow3.flac",
				data:   []byte(`uploaded together`),
			},
			{
				path:   "node-0000000000000003/uploads/Pluginctl/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				upload: "node-data/Pluginctl/sage-imagesampler-top-0.2.5/0000000000000003/1638576647406523064-wow3.flac.meta",
				data:   []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow3.flac"}}`),
			},
		},
		"EmptyMeta": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(`we don't know this filename`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(``),
			},
		},
		"EmptyMetaBadJSON": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(`we don't know this filename`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(`{32-}`),
			},
		},
		"MissingMetaField": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(`we don't know this filename`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(`{"ts":1638576647406523064}`),
			},
		},
		"MissingMetaFilenameField": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(`we still don't know this filename`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{}}`),
			},
		},
		"SkipPartial": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/.partial/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/.partial/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"SkipExtra": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/dir/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/dir/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"SkipMissing": {
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/imagesampler-top/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/uploads/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path: "node-000048b02d15bc7c/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
			{
				path: "data",
				data: []byte(`some data`),
			},
			{
				path: "meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
		"SkipNonUploadsRoot": {
			{
				path: "node-000048b02d15bc7c/uploads-copy/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data",
				data: []byte(`some data`),
			},
			{
				path: "node-000048b02d15bc7c/uploads-copy/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta",
				data: []byte(`{"ts":1638576647406523064,"labels":{"filename":"wow1.txt"}}`),
			},
		},
	}

	for name, testfiles := range testcases {
		t.Run(name, func(t *testing.T) {
			root := tempDirFromTestFiles(t, testfiles)

			uploader := newMockUploader()
			worker := &Worker{
				DeleteFilesAfterUpload: true,
				Uploader:               uploader,
			}

			scanAndProcessDirOnce(worker, root)

			// check that all expected uploads exist
			uploads := make(map[pair]bool)

			for _, file := range testfiles {
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

			// check that files have been cleaned up
			for k := range uploads {
				if uploader.Uploads[k] {
					if _, err := os.Stat(k.src); err == nil {
						t.Fatalf("file should have been cleaned up: %s", k.src)
					}
				}
			}
		})
	}
}
