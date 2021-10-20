package main

import (
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/streadway/amqp"
)

var index Index
var bucketMap BucketMap

var jobQueue chan Job
var broadcast chan string

var dataDirectory string

var newSession *session.Session
var svc *s3.S3

var amqp_con *amqp.Connection
var amqp_chan *amqp.Channel
var notifyCloseChannel chan *amqp.Error

var rabbitmq_host string
var rabbitmq_port string
var rabbitmq_user string
var rabbitmq_password string
var rabbitmq_exchange string
var rabbitmq_queue string
var rabbitmq_routingkey string

var rabbitmq_cacert_file string
var rabbitmq_cert_file string
var rabbitmq_key_file string

var send_rmq_message bool
var delete_files_on_success bool
var one_fs_scan_only bool
var fs_sleep_sec int

var PretendUpload = false

//var maxMemory int64
var s3bucket string

func readFilesystemLoop(files_dir string, perFileDelay time.Duration) {

	count := 0
	for {
		count = (count + 1) % 10
		log.Printf("start readFilesystem")
		err := readFilesystem(files_dir, (count == 9), (count == 9), perFileDelay)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("start readFilesystem again in %d seconds", fs_sleep_sec)
		time.Sleep(time.Second * time.Duration(fs_sleep_sec))
	}

}

func readFilesystem(files_dir string, cleanupDirectories bool, cleanupDone bool, perFileDelay time.Duration) (err error) {
	total_data_files := 0
	total_datameta_files := 0
	new_files_count := 0
	glob_str_no_namespace := filepath.Join(files_dir, "node-*", "uploads", "*", "*", "*", "data")
	glob_str_correct := filepath.Join(files_dir, "node-*", "uploads", "*", "*", "*", "*", "data") // namespace, name, version, timestamp-sha

	// makes it easier to remove the path prefix later
	if files_dir[len(files_dir)-1] != '/' {
		files_dir = files_dir + "/"
	}

	//max_files_add_in_loop := 100
	for _, glob_str := range []string{glob_str_correct, glob_str_no_namespace} {

		log.Printf("Searching for files in %s", glob_str)

		var matches []string
		matches, err = filepath.Glob(glob_str)
		if err != nil {
			err = fmt.Errorf("filepath.Glob failed: %s", err.Error())
			return
		}
		for _, m := range matches {
			total_data_files++
			//if total_data_files >= max_files_add_in_loop {
			//	return // we do not want to read too many files at once
			//}
			dir := filepath.Dir(m)

			done_filename := filepath.Join(dir, "done")
			_, err = os.Stat(done_filename)
			if err == nil {
				// done file exists, skip...
				continue
			}
			err = nil

			meta_filename := filepath.Join(dir, "meta")

			_, err = os.Stat(meta_filename)
			if err != nil {
				// meta file does not exist yet, continue...
				err = nil
				continue
			}
			total_datameta_files++

			dir = strings.TrimPrefix(dir, files_dir)

			// slow this down so we do not starve other processes
			if new_files_count%10 == 0 {
				time.Sleep(perFileDelay)
			}

			var fileAdded bool
			fileAdded, err = index.Add(dir)
			if err != nil {
				return
			}
			if fileAdded {

				new_files_count++
			}

			if fileAdded {
				if new_files_count < 20 {
					log.Println("(readFilesystem) added " + dir)

				} else if new_files_count%100 == 0 {
					log.Printf("(readFilesystem) new_files_count: %d\n", new_files_count)
				}

			}

		}
	}

	log.Printf("(readFilesystem) Total files: %d", total_datameta_files)
	log.Printf("(readFilesystem) New files added: %d", new_files_count)

	if cleanupDirectories {

		// timestamp-sha directory should not exist anymore
		globVersionDirs := filepath.Join(files_dir, "node-*", "uploads", "*", "*", "*")

		var matches []string
		matches, err = filepath.Glob(globVersionDirs)
		if err != nil {
			err = fmt.Errorf("filepath.Glob failed: %s", err.Error())
			return
		}

		now := time.Now()

		threeDaysAgo := now.AddDate(0, 0, -3)

		for _, dir := range matches {
			log.Printf("got: %s", dir)
			//dir := filepath.Dir(m)
			var finfo fs.FileInfo
			finfo, err = os.Stat(dir)
			if err != nil {
				err = nil
				continue
			}
			if !finfo.IsDir() {
				// not a directory
				continue
			}
			if finfo.ModTime().After(threeDaysAgo) {
				// not old enough
				//log.Printf("not old enough")
				continue
			}
			var isEmpty bool
			isEmpty, err = IsDirectoryEmpty(dir)
			if err != nil {
				//log.Printf("some error")
				//err = nil
				continue
			}
			if !isEmpty {
				//log.Printf("not empty")
				continue
			}

			err = os.Remove(dir)
			if err != nil {
				log.Printf("Could not remove old and empty directory: %s", err.Error())
				continue
			}
			log.Printf("deleted directory: %s", dir)
		}

	}

	// this removes "Done" enries from the index
	if cleanupDone && delete_files_on_success {
		var toBeRemoved []string
		toBeRemoved, err = index.GetList(Done)
		if err != nil {
			err = fmt.Errorf("could not not get list of done jobs: %s", err.Error())
			return
		}

		log.Printf("trying to remove %d jobs from map", len(toBeRemoved))
		count := 0
		for _, job := range toBeRemoved {
			err = index.Remove(job, "cleanupDone")
			if err != nil {
				err = fmt.Errorf("could remove job from map: %s", err.Error())
				return
			}
			//delete(index.Map, job)
			count++
		}
		log.Printf("%d jobs removed from map", count)
	}

	return
}

// source: https://stackoverflow.com/a/30708914/2069181
func IsDirectoryEmpty(name string) (bool, error) {
	myfile, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer myfile.Close()

	var files []string
	files, err = myfile.Readdirnames(1) // Or f.Readdir(1)

	for _, v := range files {
		log.Printf("name: %s", name)
		log.Printf("content: %s", v)
	}

	if err == io.EOF {
		return true, nil
	}
	return false, err // Either not empty or error, suits both cases
}

func getPendingCandidates(max_count int) (candidates []string, err error) {
	// read candidates first
	rlock, err := index.RLockNamed("fillQueue-read")
	defer index.RUnlockNamed(rlock)
	if err != nil {
		log.Fatal(err)
	}
	candidates = []string{}
	for key, value := range index.Map {
		//fmt.Printf("key: %s %s\n", key, value.String())
		if value != Pending {
			continue
		}

		candidates = append(candidates, key)
		if len(candidates) >= max_count {
			return
		}
	}

	return
}

// TODO also clean here ?
// TODO lock index and sleep for 3 seconds before cleanup, to give filesystem time to remove files
func fillQueue(candidateArrayLen int) (err error) {

	var candidates []string
	candidates, err = getPendingCandidates(candidateArrayLen)
	if err != nil {
		err = fmt.Errorf("getPendingCandidates failed: %s", err.Error())
		return
	}

	if len(candidates) == 0 {
		time.Sleep(3 * time.Second)
		return
	}

	//if we collected candidates, push into queue and update state
	count := 0
	for _, cand := range candidates {
		jobQueue <- Job(cand) // this is and should be blocking
		index.Set(cand, Active, "fillQueue")
		count++
	}
	log.Printf("%d jobs put in queue.", count)
	return
}

func fillQueueLoop(candidateArrayLen int) {

	for {
		err := fillQueue(candidateArrayLen)
		if err != nil {
			log.Fatalf("fillQueue failed: %s", err.Error())
		}
	}
}

func WaitForCtrlC() {
	var end_waiter sync.WaitGroup
	end_waiter.Add(1)
	//var signal_channel chan os.Signal
	var signal_channel = make(chan os.Signal, 1)
	signal.Notify(signal_channel, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM, // SIGTERM 15 Terminate a process gracefully
		syscall.SIGQUIT,
	)
	go func() {
		<-signal_channel
		end_waiter.Done()
	}()
	end_waiter.Wait()
}

func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
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

	useS3 := true

	if useS3 {
		configS3()
	}

	max_worker_count := getEnvInt("workers", 1) // 10 suggested for production
	queue_size := 50                            // 50 suggested for production
	candiateArrayLen := 100                     // 100 suggested for production

	fs_sleep_sec = getEnvInt("fs_sleep_sec", 3)

	fmt.Println("SAGE Uploader")

	log.Printf("max_worker_count: %d", max_worker_count)
	log.Printf("queue_size: %d", queue_size)

	send_rmq_message = getEnvBool("send_rmq_message", false)

	if send_rmq_message {
		rabbitmq_host = os.Getenv("rabbitmq_host")
		rabbitmq_port = os.Getenv("rabbitmq_port")
		rabbitmq_user = os.Getenv("rabbitmq_user")
		rabbitmq_password = os.Getenv("rabbitmq_password")
		rabbitmq_exchange = os.Getenv("rabbitmq_exchange")
		rabbitmq_queue = os.Getenv("rabbitmq_queue")
		rabbitmq_routingkey = os.Getenv("rabbitmq_routingkey")

		rabbitmq_cacert_file = os.Getenv("rabbitmq_cacert_file")
		rabbitmq_cert_file = os.Getenv("rabbitmq_cert_file")
		rabbitmq_key_file = os.Getenv("rabbitmq_key_file")

		if rabbitmq_host == "" {
			log.Fatalf("rabbitmq_host not defined")
			return
		}

		if rabbitmq_port == "" {
			log.Fatalf("rabbitmq_port not defined")
			return
		}

		if rabbitmq_cert_file == "" {
			if rabbitmq_user == "" {
				log.Fatalf("provide rabbitmq_cert_file or rabbitmq_user")
				return
			}
			if rabbitmq_password == "" {
				log.Fatalf("provide rabbitmq_cert_file or rabbitmq_password")
				return
			}
		}
		log.Printf("rabbitmq_host: %s", rabbitmq_host)
		log.Printf("rabbitmq_port: %s", rabbitmq_port)
		log.Printf("rabbitmq_user: %s", rabbitmq_user)
		log.Printf("rabbitmq_exchange: %s", rabbitmq_exchange)
		log.Printf("rabbitmq_queue: %s", rabbitmq_queue)
		log.Printf("rabbitmq_routingkey: %s", rabbitmq_routingkey)

	}

	delete_files_on_success = getEnvBool("delete_files_on_success", false)
	one_fs_scan_only = getEnvBool("one_fs_scan_only", false)

	// create channels
	jobQueue = make(chan Job, queue_size)
	broadcast = make(chan string, max_worker_count+10) // +10 just to be safe

	// create index
	index = Index{}
	index.Init("UploaderIndex")

	// create bucket map
	bucketMap = BucketMap{}
	bucketMap.Init("BucketMap")

	// populate index

	dataDirectory = getEnvString("data_dir", "/data")

	readFilesystem(dataDirectory, delete_files_on_success, delete_files_on_success, 0)
	log.Printf("Initial readFilesystem done.")

	if !one_fs_scan_only {
		go readFilesystemLoop(dataDirectory, 100*time.Millisecond)
	}
	// debug output
	//index.Print()
	if send_rmq_message {
		go amqp_connection_loop()

		counter := 0
		for amqp_chan == nil {
			counter++
			if counter >= 10 {
				log.Fatal("Did not get RMQ connection. Exit")
			}
			log.Printf("waiting for RMQ connection...")
			time.Sleep(3 * time.Second)
		}
	}

	// this process feeds workers with work
	go fillQueueLoop(candiateArrayLen)

	// start upload workers
	wg := new(sync.WaitGroup)
	wg.Add(max_worker_count)
	workers := make([]*Worker, max_worker_count)
	for i := 1; i <= max_worker_count; i++ {
		workers[i-1] = &Worker{ID: i, wg: wg, jobQueue: jobQueue, broadcast: broadcast}
		go workers[i-1].Run()
	}

	time.Sleep(time.Second * 1)

	fmt.Printf("Press Ctrl+C to end  (This will give workers 10 seconds to complete)\n")
	WaitForCtrlC()
	fmt.Printf("\n")

	close(jobQueue)
	fmt.Println("jobQueue closed")

	//broadcast stop
	for i := 1; i <= max_worker_count+10; i++ {
		broadcast <- "STOP"
		fmt.Println("sending STOP signal")
	}
	fmt.Println("STOP signals sent")

	if waitTimeout(wg, time.Second*10) {
		fmt.Println("Timed out waiting for workers. Exit Anyway.")
	} else {
		fmt.Println("All workers finished gracefully. Exit.")
	}

	os.Exit(0)

}
