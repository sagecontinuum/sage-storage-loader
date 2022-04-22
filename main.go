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

var delete_files_on_success bool

//var maxMemory int64
var s3bucket string

const DoneFilename = ".done"

// func readFilesystem(files_dir string, cleanupDirectories bool, cleanupDone bool, perFileDelay time.Duration) error {
// 	total_data_files := 0
// 	total_datameta_files := 0
// 	new_files_count := 0

// 	patterns := []string{
// 		filepath.Join(files_dir, "node-*", "uploads", "*", "*", "*", "data"),      // uploads without a namespace
// 		filepath.Join(files_dir, "node-*", "uploads", "*", "*", "*", "*", "data"), // uploads with a namespace
// 	}

// 	// makes it easier to remove the path prefix later
// 	if !strings.HasSuffix(files_dir, "/") {
// 		files_dir += "/"
// 	}

// 	err := filepath.WalkDir(files_dir, func(path string, d fs.DirEntry, err error) error {
// 		for _, pattern := range patterns {
// 			ok, err := filepath.Match(pattern, path)
// 			if err != nil {
// 				return err
// 			}
// 			if !ok {
// 				return nil
// 			}

// 			total_data_files++

// 			dir := filepath.Dir(path)

// 			// skip if done file exists
// 			if _, err := os.Stat(filepath.Join(dir, DoneFilename)); err == nil {
// 				return filepath.SkipDir
// 			}

// 			// skip if meta file doesn't exist
// 			if _, err := os.Stat(filepath.Join(dir, "meta")); errors.Is(err, os.ErrNotExist) {
// 				return filepath.SkipDir
// 			}

// 			total_datameta_files++

// 			dir = strings.TrimPrefix(dir, files_dir)

// 			// slow this down so we do not starve other processes
// 			if new_files_count%10 == 0 {
// 				time.Sleep(perFileDelay)
// 			}

// 			fileAdded, err := index.Add(dir)
// 			if err != nil {
// 				return err
// 			}

// 			if fileAdded {
// 				new_files_count++
// 				if new_files_count < 20 {
// 					log.Println("(readFilesystem) added " + dir)

// 				} else if new_files_count%100 == 0 {
// 					log.Printf("(readFilesystem) new_files_count: %d\n", new_files_count)
// 				}
// 			}
// 		}
// 		return nil
// 	})

// 	if err != nil {
// 		return err
// 	}

// 	// TODO(maybe just add a file garbage collector step periodically)

// 	log.Printf("(readFilesystem) Total files: %d", total_datameta_files)
// 	log.Printf("(readFilesystem) New files added: %d", new_files_count)

// 	// if cleanupDirectories {
// 	// 	// timestamp-sha directory should not exist anymore
// 	// 	globVersionDirs := filepath.Join(files_dir, "node-*", "uploads", "*", "*", "*")

// 	// 	matches, err := filepath.Glob(globVersionDirs)
// 	// 	if err != nil {
// 	// 		return fmt.Errorf("filepath.Glob failed: %s", err.Error())
// 	// 	}

// 	// 	now := time.Now()

// 	// 	threeDaysAgo := now.AddDate(0, 0, -3)

// 	// 	for _, dir := range matches {
// 	// 		log.Printf("got: %s", dir)
// 	// 		//dir := filepath.Dir(m)
// 	// 		var finfo fs.FileInfo
// 	// 		finfo, err = os.Stat(dir)
// 	// 		if err != nil {
// 	// 			err = nil
// 	// 			continue
// 	// 		}
// 	// 		if !finfo.IsDir() {
// 	// 			// not a directory
// 	// 			continue
// 	// 		}
// 	// 		if finfo.ModTime().After(threeDaysAgo) {
// 	// 			// not old enough
// 	// 			//log.Printf("not old enough")
// 	// 			continue
// 	// 		}
// 	// 		var isEmpty bool
// 	// 		isEmpty, err = IsDirectoryEmpty(dir)
// 	// 		if err != nil {
// 	// 			//log.Printf("some error")
// 	// 			//err = nil
// 	// 			continue
// 	// 		}
// 	// 		if !isEmpty {
// 	// 			//log.Printf("not empty")
// 	// 			continue
// 	// 		}

// 	// 		err = os.Remove(dir)
// 	// 		if err != nil {
// 	// 			log.Printf("Could not remove old and empty directory: %s", err.Error())
// 	// 			continue
// 	// 		}
// 	// 		log.Printf("deleted directory: %s", dir)
// 	// 	}
// 	// }

// 	// // this removes "Done" enries from the index
// 	// if cleanupDone && delete_files_on_success {
// 	// 	toBeRemoved, err := index.GetList(Done)
// 	// 	if err != nil {
// 	// 		return fmt.Errorf("could not not get list of done jobs: %s", err.Error())
// 	// 	}

// 	// 	log.Printf("trying to remove %d jobs from map", len(toBeRemoved))
// 	// 	count := 0
// 	// 	for _, job := range toBeRemoved {
// 	// 		if err := index.Remove(job, "cleanupDone"); err != nil {
// 	// 			return fmt.Errorf("could remove job from map: %s", err.Error())
// 	// 		}
// 	// 		//delete(index.Map, job)
// 	// 		count++
// 	// 	}
// 	// 	log.Printf("%d jobs removed from map", count)
// 	// }

// 	return nil
// }

// // source: https://stackoverflow.com/a/30708914/2069181
// func IsDirectoryEmpty(name string) (bool, error) {
// 	myfile, err := os.Open(name)
// 	if err != nil {
// 		return false, err
// 	}
// 	defer myfile.Close()

// 	var files []string
// 	files, err = myfile.Readdirnames(1) // Or f.Readdir(1)

// 	for _, v := range files {
// 		log.Printf("name: %s", name)
// 		log.Printf("content: %s", v)
// 	}

// 	if err == io.EOF {
// 		return true, nil
// 	}
// 	return false, err // Either not empty or error, suits both cases
// }

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

	delete_files_on_success = getEnvBool("delete_files_on_success", false)

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
				DeleteFilesOnSuccess: delete_files_on_success,
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
