package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

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

func worker(id int, wg *sync.WaitGroup, jobQueue <-chan Job, broadcast <-chan string) {
	defer wg.Done()
	fmt.Printf("Worker %d starting\n", id)

FOR:
	for {
		// by splitting this into two selects, is is guaranteed that the broadcast is not skipped
		select {
		case signal := <-broadcast:
			if signal == "STOP" {
				fmt.Printf("Worker %d received STOP signal.\n", id)
				break FOR
			}
		default:
		}

		select {
		case job := <-jobQueue:
			err := processing(id, job)
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
	fmt.Printf("Worker %d stopping.\n", id)
}

type pInfo struct {
	NodeID    string
	Namespace string
	Name      string
	Version   string
}

func parseUploadPath(dir string) (p *pInfo, err error) {

	p = &pInfo{}
	dir_array := strings.Split(dir, "/")

	p.NodeID = strings.TrimPrefix(dir_array[0], "node-")
	p.Namespace = "sage" // sage is the default in cases where no namespace was given
	p.Name = ""
	p.Version = ""
	if len(dir_array) == 6 {
		p.Namespace = dir_array[2]
		p.Name = dir_array[3]
		p.Version = dir_array[4]
	} else if len(dir_array) == 5 { // namespace is missing
		p.Name = dir_array[2]
		p.Version = dir_array[3]
	} else {
		err = fmt.Errorf("could not parse path %s", dir)
		return
	}

	return
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
	full_dir := filepath.Join(Directory, dir)

	var p *pInfo
	p, err = parseUploadPath(dir)
	if err != nil {
		return
	}

	fmt.Printf("Worker %d: got node_id %s\n", id, p.NodeID)
	fmt.Printf("Worker %d: got plugin_namespace %s\n", id, p.Namespace)
	fmt.Printf("Worker %d: got plugin_name %s\n", id, p.Name)
	fmt.Printf("Worker %d: got plugin_version %s\n", id, p.Version)

	var meta *MetaData
	meta, err = getMetadata(full_dir)
	if err != nil {
		return
	}

	meta.Name = "upload"
	//spew.Dump(meta)

	fmt.Printf("Worker %d: got shasum %s\n", id, meta.Shasum)
	if meta.EpochNanoOld != nil {
		meta.EpochNano = *meta.EpochNanoOld
		meta.EpochNanoOld = nil
	}
	fmt.Printf("Worker %d: got EpochNano %d\n", id, meta.EpochNano)

	if meta.Meta == nil { // read Labels only if Meta is empty
		meta.Meta = meta.Labels
		meta.Labels = nil
	}
	meta.Shasum = nil

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

	timestamp := time.Unix(meta.EpochNano/1e9, meta.EpochNano%1e9)
	fmt.Printf("Worker %d: got timestamp %s\n", id, timestamp)

	timestamp_date := timestamp.Format("20060201")

	bucket_name := fmt.Sprintf("%s-%s-%s-%s-%s", p.NodeID, p.Namespace, p.Name, p.Version, timestamp_date)
	log.Printf("bucket_name: %s", bucket_name)
	bucket_id := ""
	bucket_id, err = getOrCreateBucket(bucket_name)
	if err != nil {
		err = fmt.Errorf("getOrCreateBucket failed: %s", err.Error())
		return
	}

	log.Printf("bucket_id: %s", bucket_id)

	uploadTarget := "s3"

	targetNameData := fmt.Sprintf("%d-%s", meta.EpochNano, labelFilename)
	targetNameMeta := fmt.Sprintf("%d-%s.meta", meta.EpochNano, labelFilename)

	dataFileLocal := filepath.Join(full_dir, "data")
	metaFileLocal := filepath.Join(full_dir, "meta")

	var sageFileUrl string
	sageFileUrl, err = uploadFile(uploadTarget, bucket_id, dataFileLocal, targetNameData)
	if err != nil {
		return
	}
	fmt.Printf("upload success: %s %s\n", bucket_id, targetNameData)
	_, err = uploadFile(uploadTarget, bucket_id, metaFileLocal, targetNameMeta)
	if err != nil {
		return
	}

	fmt.Printf("upload success: %s %s\n", bucket_id, targetNameMeta)

	meta.Value = sageFileUrl

	//send message
	var jsonBytes []byte
	jsonBytes, err = json.Marshal(meta)
	if err != nil {
		return
	}

	err = notify_message(p.NodeID, jsonBytes)
	if err != nil {
		return
	}
	fmt.Printf("RMQ message sent %s\n", sageFileUrl)

	// *** delete files

	err = os.RemoveAll(full_dir)
	if err != nil {
		err = fmt.Errorf("can not delete directory (%s): %s", full_dir, err.Error())
		return
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
