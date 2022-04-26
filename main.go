package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

const DoneFilename = ".done"

func scanForJobs(stop <-chan struct{}, jobs chan<- Job, root string) error {
	patterns := []string{
		filepath.Join(root, "node-*", "uploads", "*", "*", "*", "data"),      // uploads without a namespace
		filepath.Join(root, "node-*", "uploads", "*", "*", "*", "*", "data"), // uploads with a namespace
	}

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-stop:
			return fmt.Errorf("walk stopped")
		default:
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
				return fmt.Errorf("walk stopped")
			}
		}

		return nil
	})
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

func mustGetenv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("env var must be defined: %s", key)
	}
	return val
}

func mustGetS3Uploader() *S3FileUploader {
	endpoint := mustGetenv("s3Endpoint")
	accessKeyID := mustGetenv("s3accessKeyID")
	secretAccessKey := mustGetenv("s3secretAccessKey")
	bucket := mustGetenv("s3bucket")

	region := "us-west-2"
	//region := "us-east-1" // minio default
	disableSSL := !strings.HasPrefix(endpoint, "https")
	//log.Printf("HasPrefix: %t", strings.HasPrefix(s3Endpoint, "https"))
	//log.Printf("disableSSL: %t", disableSSL)
	//os.Exit(0)
	//maxMemory = 32 << 20 // 32Mb

	log.Printf("creating s3 session at %s@%s in bucket %s", accessKeyID, endpoint, bucket)

	session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(region),
		DisableSSL:       aws.Bool(disableSSL),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		log.Fatalf("failed to create s3 session: %s", err.Error())
	}

	return &S3FileUploader{
		Session: session,
		Bucket:  bucket,
	}
}

func getEnvString(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		i, _ := strconv.Atoi(value)
		return i
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		if strings.ToLower(value) == "true" {
			return true
		}
		if value == "1" {
			return true
		}
		return false
	}
	return fallback
}

func main() {
	// TODO: add garbage collection/notifier if old files are not moved away
	useS3 := false

	numWorkers := getEnvInt("workers", 1) // 10 suggested for production

	log.Println("SAGE Uploader")

	deleteFilesOnSuccess := getEnvBool("delete_files_on_success", true)

	// dataRoot := getEnvString("data_dir", "/data")
	dataRoot := getEnvString("data_dir", "test-data")

	stop := make(chan struct{})

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	go func() {
		<-sig
		log.Printf("stopping...")
		close(stop)
	}()

	jobs, errc := fillJobQueue(stop, dataRoot)

	results := make(chan string)

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()

			// setup worker's uploader
			var uploader FileUploader

			if useS3 {
				uploader = mustGetS3Uploader()
			} else {
				uploader = &LogFileUploader{}
			}

			worker := &Worker{
				DeleteFilesOnSuccess: deleteFilesOnSuccess,
				Uploader:             uploader,
				Jobs:                 jobs,
				Results:              results,
				Stop:                 stop,
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
		if err != nil {
			log.Printf("error: %s", err.Error())
		}
	default:
	}
}
