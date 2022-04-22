package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWorkerProcess(t *testing.T) {
	testcases := map[string]struct {
		srcDir, dst string
	}{
		// NOTE(sean) the filename comes from data generated by newTempDir
		"default-sage": {
			srcDir: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c",
			dst:    "node-data/sage/imagesampler-top-0.2.5/000048b02d15bc7c/1638576647406523064-wow1.txt",
		},
		"namespace": {
			srcDir: "node-000048b02d15bc7d/uploads/namespace/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c",
			dst:    "node-data/namespace/imagesampler-top-0.2.5/000048b02d15bc7d/1638576647406523064-wow4.txt",
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			dataDirectory = newTempDir(t)

			// ah... just write the files here...

			uploader := NewMockUploader()

			w := &Worker{
				ID:       123,
				Uploader: uploader,
			}

			if err := w.Process(Job(tc.srcDir)); err != nil {
				t.Fatal(err)
			}

			if !uploader.WasUploaded(filepath.Join(dataDirectory, tc.srcDir, "data"), tc.dst) {
				t.Fatalf("missing upload\nsrc: %s\ndst: %s\n\n%v", tc.srcDir, tc.dst, uploader.uploads)
			}

			if !uploader.WasUploaded(filepath.Join(dataDirectory, tc.srcDir, "meta"), tc.dst+".meta") {
				t.Fatalf("missing upload\nsrc: %s\ndst: %s", tc.srcDir, tc.dst)
			}
		})
	}
}

func TestParseUploadPath(t *testing.T) {
	testcases := map[string]struct {
		Dir  string
		Info pInfo
	}{
		"default": {
			Dir: "node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-8ca463ebcfab357f5d07c2529fb3939ddb4a5c32",
			Info: pInfo{
				Namespace: "sage",
				Name:      "imagesampler-top",
				Version:   "0.2.5",
				NodeID:    "000048b02d15bc7c",
			},
		},
		"namespace1": {
			Dir: "node-000048b02d15bc7d/uploads/Pluginctl/imagesampler-top/1.2.7/1638576647406523064-8ca463ebcfab357f5d07c2529fb3939ddb4a5c32",
			Info: pInfo{
				Namespace: "Pluginctl",
				Name:      "imagesampler-top",
				Version:   "1.2.7",
				NodeID:    "000048b02d15bc7d",
			},
		},
		"namespace2": {
			Dir: "node-00004cd98fc686c9/uploads/smoke-detector-1650456133/plugin-test-pipeline-0-2-8-1f055011/0.2.8/1649087778906567900-a31446e4291ac3a04a3c331e674252a63ee95604",
			Info: pInfo{
				Namespace: "smoke-detector-1650456133",
				Name:      "plugin-test-pipeline-0-2-8-1f055011",
				Version:   "0.2.8",
				NodeID:    "00004cd98fc686c9",
			},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			info, err := parseUploadPath(tc.Dir)
			if err != nil {
				t.Fatal(err)
			}
			assertStringsEqual(t, info.Namespace, tc.Info.Namespace)
			assertStringsEqual(t, info.Name, tc.Info.Name)
			assertStringsEqual(t, info.Version, tc.Info.Version)
			assertStringsEqual(t, info.NodeID, tc.Info.NodeID)
		})
	}
}

func assertStringsEqual(t *testing.T, got, want string) {
	if want != got {
		t.Fatalf("strings don't match. want: %q got: %q", want, got)
	}
}

func writeFile(name string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
		return err
	}
	return os.WriteFile(name, data, 0o644)
}

func newTempDir(t *testing.T) string {
	root := filepath.Join(t.TempDir(), "data")

	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}

	items := map[string][]byte{
		"node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data": []byte(`testing`),
		"node-000048b02d15bc7c/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta": []byte(`{"ts":1638576647406523064,"meta":{"filename":"wow1.txt"}}`),

		"node-000048b02d15bc7c/uploads/imagesampler-top/0.2.6/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data": []byte("testing"),
		"node-000048b02d15bc7c/uploads/imagesampler-top/0.2.6/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta": []byte(`{"ts":1638576647406523064,"meta":{"filename":"wow2.txt"}}`),

		"node-000048b02d15bc7d/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data": []byte("testing"),
		"node-000048b02d15bc7d/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta": []byte(`{"ts":1638576647406523064,"meta":{"filename":"wow3.txt"}}`),

		"node-000048b02d15bc7d/uploads/namespace/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/data": []byte("testing in namespace"),
		"node-000048b02d15bc7d/uploads/namespace/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/meta": []byte(`{"ts":1638576647406523064,"meta":{"filename":"wow4.txt"}}`),

		"node-000048b02d15bc7d/uploads/imagesampler-top/0.2.5/1638576647406523064-9801739daae44ec5293d4e1f53d3f4d2d426d91c/.partial/hello": []byte("!!! you should never see me !!!"),
	}

	for name, data := range items {
		if err := writeFile(filepath.Join(root, name), data); err != nil {
			panic(err)
		}
	}

	return root
}

type pair struct{ src, dst string }

type MockUploader struct {
	Error   error
	uploads map[pair]bool
}

func NewMockUploader() *MockUploader {
	return &MockUploader{
		uploads: make(map[pair]bool),
	}
}

func (up *MockUploader) UploadFile(src, dst string, meta map[string]string) error {
	if up.Error != nil {
		return up.Error
	}
	up.uploads[pair{src, dst}] = true
	return nil
}

func (up *MockUploader) WasUploaded(src, dst string) bool {
	return up.uploads[pair{src, dst}]
}
