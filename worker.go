package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Worker struct {
	ID       int
	Skipped  int64
	wg       *sync.WaitGroup
	jobQueue <-chan Job
	shutdown <-chan int
}

type MetaData struct {
	Name         string                 `json:"name"` // always set as "upload"
	EpochNano    int64                  `json:"ts,omitempty"`
	EpochNanoOld *int64                 `json:"timestamp,omitempty"` // only for reading (deprecated soon), keep it backwards compatible
	Shasum       *string                `json:"shasum,omitempty"`
	Labels       map[string]interface{} `json:"labels,omitempty"` // only read (will write to meta)
	Meta         map[string]interface{} `json:"meta"`
	Value        string                 `json:"val"` // only used to output URL, input is assumed to be empty
}

// ErrorStruct _
type ErrorStruct struct {
	Error string `json:"error,omitempty"`
}

var sage_storage_api = "http://host.docker.internal:8080"
var sage_storage_token = "user:test"
var sage_storage_username = "test"

func (worker *Worker) Run() {
	defer worker.wg.Done()
	fmt.Printf("Worker %d starting\n", worker.ID)

FOR:
	for {
		// by splitting this into two selects, is is guaranteed that the broadcast is not skipped
		select {
		case <-worker.shutdown:
			fmt.Printf("Worker %d received shutdown signal.\n", worker.ID)
			break FOR
		default:
		}

		select {
		case job := <-worker.jobQueue:
			err := processing(worker.ID, job)
			if err != nil {
				log.Printf("Somthing went wrong: %s", err.Error())
				index.Set(string(job), Failed, "worker")
				err = nil
			} else {
				index.Set(string(job), Done, "worker")
			}
		default:
			// there is no work, slow down..
			time.Sleep(time.Second)
		}
	}
	fmt.Printf("Worker %d stopping.\n", worker.ID)
}

type pInfo struct {
	NodeID    string
	Namespace string
	Name      string
	Version   string
}

func parseUploadPath(dir string) (*pInfo, error) {
	dir_array := strings.Split(dir, "/")

	p := &pInfo{
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

func getMetadata(full_dir string) (m *MetaData, err error) {
	var content []byte
	content, err = ioutil.ReadFile(filepath.Join(full_dir, "meta"))
	if err != nil {
		err = fmt.Errorf("ioutil.ReadFile failed: %s", err.Error())
		return
	}

	meta := MetaData{}
	err = json.Unmarshal(content, &meta)
	if err != nil {
		err = fmt.Errorf("parsing metdata failed: %s (%s)", err.Error(), string(content))
		return
	}
	m = &meta
	return
}

func processing(id int, job Job) (err error) {
	fmt.Printf("Worker %d: processing job %s\n", id, string(job))
	dir := string(job) // starts with  node-000048b02d...
	full_dir := filepath.Join(dataDirectory, dir)

	// if !delete_files_on_success {
	// 	if _, err = os.Stat(flag_file); err == nil {
	// 		// already exists
	// 		//fmt.Printf("Worker %d: Flag file found, skipping upload.\n", id)
	// 		return
	// 	}
	// }

	err = nil

	var p *pInfo
	p, err = parseUploadPath(dir)
	if err != nil {
		return
	}

	//fmt.Printf("Worker %d: got node_id %s\n", id, p.NodeID)
	//fmt.Printf("Worker %d: got plugin_namespace %s\n", id, p.Namespace)
	//fmt.Printf("Worker %d: got plugin_name %s\n", id, p.Name)
	//fmt.Printf("Worker %d: got plugin_version %s\n", id, p.Version)

	var meta *MetaData
	meta, err = getMetadata(full_dir)
	if err != nil {
		return
	}

	meta.Name = "upload"
	//spew.Dump(meta)

	//fmt.Printf("Worker %d: got shasum %s\n", id, *meta.Shasum)
	if meta.EpochNanoOld != nil {
		meta.EpochNano = *meta.EpochNanoOld
		meta.EpochNanoOld = nil
	}
	//fmt.Printf("Worker %d: got EpochNano %d\n", id, meta.EpochNano)

	//spew.Dump(meta)
	if meta.Meta == nil { // read Labels only if Meta is empty
		meta.Meta = meta.Labels
		meta.Labels = nil
	}
	meta.Shasum = nil
	//spew.Dump(meta)

	labelFilenameIf, ok := meta.Meta["filename"]
	if !ok {
		err = fmt.Errorf("label field  filename is missing")
		return
	}

	labelFilename, ok := labelFilenameIf.(string)
	if !ok {
		err = fmt.Errorf("label field filename is not a string")
		return
	}
	if len(labelFilename) == 0 {
		err = fmt.Errorf("label field filename is empty")
		return
	}

	// add info extracted from path
	meta.Meta["node"] = strings.ToLower(p.NodeID)
	meta.Meta["plugin"] = p.Namespace + "/" + p.Name + ":" + p.Version

	//bucket_id := ""

	uploadTarget := "s3"

	targetNameData := fmt.Sprintf("%d-%s", meta.EpochNano, labelFilename)
	targetNameMeta := fmt.Sprintf("%d-%s.meta", meta.EpochNano, labelFilename)

	dataFileLocal := filepath.Join(full_dir, "data")
	metaFileLocal := filepath.Join(full_dir, "meta")

	//TODO:  rootFolder via config ,  jobID and instanceID(task) from json

	rootFolder := "node-data"
	jobID := "sage"
	instanceID := p.Namespace + "-" + p.Name + "-" + p.Version
	//outputID := "default"
	s3path := fmt.Sprintf("%s/%s/%s/%s", rootFolder, jobID, instanceID, p.NodeID)

	var s3metadata map[string]string
	skipUpload := false
	if uploadTarget == "s3" {

		s3metadata = make(map[string]string)

		s3metadata["name"] = meta.Name
		s3metadata["ts"] = strconv.FormatInt(meta.EpochNano, 10)
		if meta.Shasum != nil {
			s3metadata["shasum"] = *meta.Shasum
		}
		//s3metadata["val"] = meta.Value

		for key, value := range meta.Meta {

			value_str, ok := value.(string)
			if ok {
				s3metadata["meta."+key] = value_str
			} else {
				fmt.Printf("Did not add metdata %s", key)
			}
		}

		if !PretendUpload {
			s3key := filepath.Join(s3path, targetNameData)

			// check and skip if file exists
			input := &s3.ListObjectsV2Input{
				Bucket:  aws.String(s3bucket),
				Prefix:  aws.String(s3key),
				MaxKeys: aws.Int64(2),
			}

			var result *s3.ListObjectsV2Output
			result, err = svc.ListObjectsV2(input)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					switch aerr.Code() {
					case s3.ErrCodeNoSuchBucket:
						fmt.Println(s3.ErrCodeNoSuchBucket, aerr.Error())
					default:
						fmt.Println(aerr.Error())
					}
				} else {
					// Print the error, cast err to awserr.Error to get the Code and
					// Message from an error.
					fmt.Println(err.Error())
				}
				err = nil
			} else {

				//fmt.Println(result)

				if len(result.Contents) == 2 {
					fmt.Println("Files already exist in S3")
					skipUpload = true
				}
			}
		}
	}

	if !skipUpload {
		var sageFileUrl string
		sageFileUrl, err = uploadFile(uploadTarget, s3path, dataFileLocal, targetNameData, s3metadata)
		if err != nil {
			return
		}

		_, err = uploadFile(uploadTarget, s3path, metaFileLocal, targetNameMeta, nil)
		if err != nil {
			return
		}

		fmt.Printf("upload success: %s %s (and .meta)\n", s3path, targetNameData)

		meta.Value = sageFileUrl

		if send_rmq_message {
			//send message
			var jsonBytes []byte
			jsonBytes, err = json.Marshal(meta)
			if err != nil {
				return
			}

			// ToLower should not be needed, just to be safe
			err = send_amqp_message("node-"+strings.ToLower(p.NodeID), jsonBytes)
			if err != nil {
				return
			}
			fmt.Printf("RMQ message sent %s\n", sageFileUrl)
		}
	}
	// *** delete files
	if delete_files_on_success {
		err = os.RemoveAll(full_dir)
		if err != nil {
			err = fmt.Errorf("can not delete directory (%s): %s", full_dir, err.Error())
			return
		}
	} else {

		flag_file := filepath.Join(full_dir, "done")

		var emptyFlagFile *os.File
		emptyFlagFile, err = os.Create(flag_file)
		if err != nil {
			err = fmt.Errorf("could not create flag file: %s", err.Error())
			return

		}
		//log.Println(emptyFile)
		emptyFlagFile.Close()

	}

	return
}

func run_command(cmd_str string, return_stdout bool) (output string, err error) {

	//cmd_str := strings.Join(cmd_array, " ")
	log.Printf("Command execute: %s", cmd_str)

	//cmd := exec.Command(cmd_array[0], cmd_array[1:len(cmd_array)-1]...)
	cmd := exec.Command("bash", "-c", cmd_str)

	var output_b []byte
	output_b, err = cmd.CombinedOutput()

	if err != nil {
		err = fmt.Errorf("exec.Command failed: %s", err.Error())
		return
	}
	output = string(output_b[:])

	return
}
