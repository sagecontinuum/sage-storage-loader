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
	"github.com/aws/aws-sdk-go/service/s3"
)

var newSession *session.Session
var svc *s3.S3

//var maxMemory int64
var s3bucket string

const DoneFilename = ".done"

func scanForJobs(stop <-chan struct{}, jobs chan<- Job, root string) error {
	patterns := []string{
		filepath.Join(root, "node-*", "uploads", "*", "*", "*", "data"),      // uploads without a namespace
		filepath.Join(root, "node-*", "uploads", "*", "*", "*", "*", "data"), // uploads with a namespace
	}

	// makes it easier to remove the path prefix later
	if !strings.HasSuffix(root, "/") {
		root += "/"
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
				return err
			}
			if !ok {
				return nil
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

			// relpath = filepath.Rel(root, path)
			dir = strings.TrimPrefix(dir, root)

			select {
			case jobs <- Job(dir):
			case <-stop:
				return fmt.Errorf("walk stopped")
			}
		}

		return nil
	})

	// TODO(maybe just add a file garbage collector step periodically)
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

func configS3() {
	/*
		export s3Endpoint=http://minio:9000
		export s3accessKeyID=minio
		export s3secretAccessKey=minio123
		export s3bucket=sage
	*/

	/*
		export s3Endpoint=http://host.docker.internal:9001
		export s3accessKeyID=minio
		export s3secretAccessKey=minio123
		export s3bucket=sage
	*/
	var s3Endpoint string
	var s3accessKeyID string
	var s3secretAccessKey string

	//flag.StringVar(&s3Endpoint, "s3Endpoint", "", "")
	//flag.StringVar(&s3accessKeyID, "s3accessKeyID", "", "")
	//flag.StringVar(&s3secretAccessKey, "s3secretAccessKey", "", "")
	s3Endpoint = os.Getenv("s3Endpoint")
	s3accessKeyID = os.Getenv("s3accessKeyID")
	s3secretAccessKey = os.Getenv("s3secretAccessKey")
	s3bucket = os.Getenv("s3bucket")

	log.Printf("s3Endpoint: %s", s3Endpoint)
	log.Printf("s3accessKeyID: %s", s3accessKeyID)
	log.Printf("s3bucket: %s", s3bucket)

	if s3Endpoint == "" {
		log.Fatalf("s3Endpoint not defined")
		return
	}

	if s3bucket == "" {
		log.Fatalf("s3bucket not defined")
		return
	}

	if s3accessKeyID == "" {
		log.Fatalf("s3accessKeyID not defined")
		return
	}

	if s3secretAccessKey == "" {
		log.Fatalf("s3secretAccessKey not defined")
		return
	}

	region := "us-west-2"
	//region := "us-east-1" // minio default
	disableSSL := !strings.HasPrefix(s3Endpoint, "https")
	//log.Printf("HasPrefix: %t", strings.HasPrefix(s3Endpoint, "https"))
	//log.Printf("disableSSL: %t", disableSSL)
	//os.Exit(0)
	s3FPS := true
	//maxMemory = 32 << 20 // 32Mb

	// Initialize s3
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(s3accessKeyID, s3secretAccessKey, ""),
		Endpoint:         aws.String(s3Endpoint),
		Region:           aws.String(region),
		DisableSSL:       aws.Bool(disableSSL),
		S3ForcePathStyle: aws.Bool(s3FPS),
	}

	var err error
	newSession, err = session.NewSession(s3Config) // session.New(s3Config)
	if err != nil {
		log.Fatalf("session.NewSession failed: %s", err.Error())
	}
	svc = s3.New(newSession)
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

	var uploader FileUploader

	if useS3 {
		configS3()
	} else {
		uploader = &TestUploader{}
	}

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
			worker := &Worker{
				DataRoot:             dataRoot,
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
