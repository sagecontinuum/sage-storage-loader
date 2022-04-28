package main

import (
	"errors"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const DoneFilename = ".done"

var (
	errWalkDirStopped = errors.New("walk stopped")
)

const versionPattern = "[0-9]*.[0-9]*.[0-9]*"
const timeShasumPattern = "[0-9]*-[0-9a-f]*"

func scanForJobs(stop <-chan struct{}, jobs chan<- Job, root string) error {
	patterns := []string{
		filepath.Join(root, "node-*", "uploads", "*", versionPattern, timeShasumPattern, "data"),      // uploads without a namespace
		filepath.Join(root, "node-*", "uploads", "*", "*", versionPattern, timeShasumPattern, "data"), // uploads with a namespace
	}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-stop:
			return errWalkDirStopped
		default:
		}

		if strings.HasPrefix(filepath.Base(path), ".") {
			return fs.SkipDir
		}

		for _, pattern := range patterns {
			ok, err := filepath.Match(pattern, path)
			if err != nil {
				// According to the docs, this will only fail if the pattern is invalid. I just fail here
				// since there's no way to recover and it will indicate a typo in the pattern list.
				log.Fatalf("bad glob pattern: %s", err.Error())
			}
			if !ok {
				continue
			}

			dir := filepath.Dir(path)

			// skip if done file exists
			if _, err := os.Stat(filepath.Join(dir, DoneFilename)); err == nil {
				return filepath.SkipDir
			}

			// skip if meta file doesn't exist
			if _, err := os.Stat(filepath.Join(dir, "meta")); errors.Is(err, os.ErrNotExist) {
				return filepath.SkipDir
			}

			reldir, err := filepath.Rel(root, dir)
			if err != nil {
				return err
			}

			select {
			case jobs <- Job{Root: root, Dir: reldir}:
			case <-stop:
				return errWalkDirStopped
			}
		}

		return nil
	})

	if errors.Is(err, errWalkDirStopped) {
		return nil
	}

	return err
}

func fillJobQueue(stop <-chan struct{}, root string) (<-chan Job, <-chan error) {
	jobs := make(chan Job)
	errc := make(chan error, 1)

	go func() {
		defer close(jobs)
		for {
			log.Printf("scanning for jobs...")
			if err := scanForJobs(stop, jobs, root); err != nil {
				errc <- err
				return
			}
			log.Printf("done scanning for jobs!")

			select {
			case <-time.After(10 * time.Second):
			case <-stop:
				return
			}
		}
	}()

	return jobs, errc
}

func ScanAndProcessDir(config LoaderConfig) error {
	stop := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		log.Printf("stopping...")
		close(stop)
	}()

	jobs, errc := fillJobQueue(stop, config.DataDir)

	results := make(chan string)

	var wg sync.WaitGroup
	wg.Add(config.NumWorkers)

	for i := 0; i < config.NumWorkers; i++ {
		go func() {
			defer wg.Done()

			uploader, err := NewS3FileUploader(config.S3Config)
			if err != nil {
				log.Fatalf("failed to create s3 uploader: %s", err.Error())
			}

			worker := &Worker{
				DeleteFilesAfterUpload: config.DeleteFilesAfterUpload,
				Uploader:               uploader,
				Jobs:                   jobs,
				Results:                results,
				Stop:                   stop,
			}
			worker.Run()
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for r := range results {
		log.Printf("processed %s", r)
	}

	select {
	case err := <-errc:
		return err
	default:
		return nil
	}
}

type LoaderConfig struct {
	DataDir                string
	S3Config               S3FileUploaderConfig
	DeleteFilesAfterUpload bool
	NumWorkers             int
}

func mustGetS3UploaderConfig() S3FileUploaderConfig {
	return S3FileUploaderConfig{
		Endpoint:        mustGetEnv("LOADER_S3_ENDPOINT"),
		AccessKeyID:     mustGetEnv("LOADER_S3_ACCESS_KEY_ID"),
		SecretAccessKey: mustGetEnv("LOADER_S3_SECRET_ACCESS_KEY"),
		Bucket:          mustGetEnv("LOADER_S3_BUCKET"),
		Region:          "us-west-2",
	}
}

func main() {
	config := LoaderConfig{
		NumWorkers:             mustParseInt(getEnv("LOADER_NUM_WORKERS", "1")),
		DeleteFilesAfterUpload: mustParseBool(getEnv("LOADER_DELETE_FILES_AFTER_UPLOAD", "true")),
		DataDir:                getEnv("LOADER_DATA_DIR", "/home-dirs"),
		S3Config:               mustGetS3UploaderConfig(),
	}

	log.Printf("using s3 at %s@%s in bucket %s", config.S3Config.AccessKeyID, config.S3Config.Endpoint, config.S3Config.Bucket)

	if err := ScanAndProcessDir(config); err != nil {
		log.Fatalf("loader stopped: %s", err.Error())
	}
}
