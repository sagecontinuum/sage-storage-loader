package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/davecgh/go-spew/spew"
)

type SAGEBucket struct {
	ErrorStruct `json:",inline"`
	ID          string     `json:"id,omitempty"`
	Name        string     `json:"name,omitempty"` // optional name (might be indentical to only file in bucket) username/bucketname
	Owner       string     `json:"owner,omitempty"`
	DataType    string     `json:"type,omitempty"`
	TimeCreated *time.Time `json:"time_created,omitempty"`
	//TimeUpdated *time.Time        `json:"time_last_updated,omitempty"`
	//Metadata    map[string]string `json:"metadata,omitempty"`
}

func uploadFile(uploadTarget string, bucket_id string, filename string, targetFilename string, meta map[string]string) (sageFileUrl string, err error) {
	switch uploadTarget {
	case "s3":
		return uploadFileToS3(bucket_id, filename, targetFilename, meta)
	case "sage":
		return uploadFileToSage(bucket_id, filename, targetFilename)
	default:
		err = fmt.Errorf("upload target %s not supported", uploadTarget)
		return
	}
}

// prefix is simply the path:  <path>/<targetFilename>
func uploadFileToS3(prefix string, filename string, targetFilename string, meta map[string]string) (sageFileUrl string, err error) {

	//s3_bucket := "sage"

	s3key := filepath.Join(prefix, targetFilename)

	md5_b64 := ""
	md5_b64, err = run_command(fmt.Sprintf("openssl dgst -md5 -binary %s | base64", filename), true)
	if err != nil {
		err = fmt.Errorf("could not compute md5 sum: %s", err.Error())
		return
	}
	md5_b64 = strings.TrimSuffix(md5_b64, "\n")

	log.Printf("md5_b64: %s\n", md5_b64)

	if PretendUpload {
		return
	}

	uploader := s3manager.NewUploader(newSession)

	f, err := os.Open(filename)
	if err != nil {
		err = fmt.Errorf("failed to open file %q, %v", filename, err)
		return
	}

	//https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#Uploader
	upi := &s3manager.UploadInput{
		Bucket:     aws.String(s3bucket),
		Key:        aws.String(s3key),
		Body:       f,
		ContentMD5: aws.String(md5_b64),
		Metadata:   aws.StringMap(meta),
	}

	// Upload the file to S3.
	result, err := uploader.Upload(upi)
	if err != nil {
		err = fmt.Errorf("failed to upload file, %v (%s)", err, spew.Sprint(upi))
		return
	}
	fmt.Printf("file uploaded to, %s\n", result.Location)

	if false {
		objectInput := s3.GetObjectInput{
			Bucket: aws.String(s3bucket),
			Key:    aws.String(filepath.Join(prefix, targetFilename)),
		}

		var out *s3.GetObjectOutput
		out, err = svc.GetObject(&objectInput)
		if err != nil {
			err = fmt.Errorf("error getting data, svc.GetObject returned: %s", err.Error())
			return
		}
		spew.Dump(out)
	}

	// construct URL for users
	//sageFileUrl = fmt.Sprintf("%s/api/v1/objects/%s/%s", sage_storage_api, bucket_id, targetFilename)
	sageFileUrl = "missing"

	return
}

func uploadFileToSage(bucket_id string, dataFilePath string, targetFilename string) (sageFileUrl string, err error) {

	sageFileUrl = fmt.Sprintf("%s/api/v1/objects/%s/%s", sage_storage_api, bucket_id, targetFilename)
	fmt.Printf("sageFileUrl: %s\n", sageFileUrl)
	err = uploadLargeFile(sageFileUrl, dataFilePath, 1048576, make(map[string]string))
	if err != nil {
		return
	}

	return
}

// Copied from https://stackoverflow.com/a/39781706/2069181 and modified
func uploadLargeFile(uri, filePath string, chunkSize int, params map[string]string) (err error) {
	//open file and retrieve info
	file, err := os.Open(filePath)
	if err != nil {
		return
	}
	fi, err := file.Stat()
	if err != nil {
		return
	}
	defer file.Close()

	//buffer for storing multipart data
	byteBuf := &bytes.Buffer{}

	//part: parameters
	mpWriter := multipart.NewWriter(byteBuf)
	for key, value := range params {
		err = mpWriter.WriteField(key, value)
		if err != nil {
			return
		}
	}

	//part: file
	_, err = mpWriter.CreateFormFile("file", fi.Name())
	if err != nil {
		return
	}
	contentType := mpWriter.FormDataContentType()

	nmulti := byteBuf.Len()
	multi := make([]byte, nmulti)
	_, err = byteBuf.Read(multi)
	if err != nil {
		return
	}

	//part: latest boundary
	//when multipart closed, latest boundary is added
	err = mpWriter.Close()
	if err != nil {
		return
	}
	nboundary := byteBuf.Len()
	lastBoundary := make([]byte, nboundary)
	_, err = byteBuf.Read(lastBoundary)
	if err != nil {
		return
	}

	//calculate content length
	totalSize := int64(nmulti) + fi.Size() + int64(nboundary)
	log.Printf("Content length = %v byte(s)\n", totalSize)

	//use pipe to pass request
	rd, wr := io.Pipe()
	defer rd.Close()

	go func() {
		defer wr.Close()

		//write multipart
		_, err = wr.Write(multi)
		if err != nil {
			return
		}

		//write file
		buf := make([]byte, chunkSize)
		for {
			n, err := file.Read(buf)
			if err != nil {
				break
			}
			_, _ = wr.Write(buf[:n])
		}
		//write boundary
		_, _ = wr.Write(lastBoundary)
	}()

	//construct request with rd
	req, err := http.NewRequest("PUT", uri, rd)
	if err != nil {
		err = fmt.Errorf("http.NewRequest PUT failed: %s", err.Error())
		return
	}
	req.Header.Add("Authorization", "sage "+sage_storage_token)
	req.Header.Set("Content-Type", contentType)
	req.ContentLength = totalSize

	//process request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		err = fmt.Errorf("client.Do failed: %s", err.Error())
		return
	}

	log.Println(resp.StatusCode)
	log.Println(resp.Header)

	body := &bytes.Buffer{}
	_, _ = body.ReadFrom(resp.Body)
	resp.Body.Close()
	log.Println(body)
	return
}

// check bucketMap
// check if bucket with same name already exists in sage storage
//   if yes: update bucketMap
// create bucket if needed
//   update bucketMap
func getOrCreateBucket(name string) (bucket_id string, err error) {
	// try to get existing id from map

	read_lock, err := bucketMap.RLockNamed("getOrCreateBucket")
	if err != nil {
		return
	}

	bucket_id, ok := bucketMap.Map[name]
	bucketMap.RUnlockNamed(read_lock)
	if ok {
		// success
		return
	}

	// get write lock
	err = bucketMap.LockNamed("getOrCreateBucket")
	if err != nil {
		err = fmt.Errorf("could not get write lock to bucketMap: %s", err.Error())
		return
	}
	defer bucketMap.Unlock()

	// check again if bucket exists, another worker might have created one in meantime
	bucket_id, ok = bucketMap.Map[name]
	if ok {
		// success
		return
	}

	// bucket id is not in cache, check if bucket exists
	bucket_id, ok, err = getSageBucketByName(name)
	if err != nil {
		err = fmt.Errorf("getSageBucketByName: %s", err.Error())
		return
	}
	if ok {
		return
	}

	// no existing bucket was found, create new bucket
	bucket_id, err = createBucket(name)
	if err != nil {
		err = fmt.Errorf("createBucket: %s", err.Error())
		return
	}

	bucketMap.Map[name] = bucket_id

	return
}

func getSageBucketByName(bucket_name string) (id string, ok bool, err error) {
	ok = false

	//bucket_type := "training-data"
	//bucket_name := "uploader_bucket"
	client := &http.Client{}
	var req *http.Request
	req, err = http.NewRequest("GET", fmt.Sprintf("%s/api/v1/objects?name=%s&owner=%s", sage_storage_api, bucket_name, sage_storage_username), nil)
	if err != nil {
		return
	}

	req.Header.Add("Authorization", "sage "+sage_storage_token)

	var resp *http.Response
	resp, err = client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		// try to read body:

		var b []byte
		b, err = io.ReadAll(resp.Body)

		if err == nil {
			err = errors.New(string(b[:]))
			return
		}
		err = fmt.Errorf("getting bucket failed, StatusCode: %d", resp.StatusCode)
		return
	}

	var b []byte
	b, err = io.ReadAll(resp.Body)
	if err != nil {
		err = fmt.Errorf("could not read body of request: %s", err.Error())
		return
	}

	bucket_list := make([]SAGEBucket, 0, 1)
	err = json.Unmarshal(b, &bucket_list)
	if err != nil {
		err = fmt.Errorf("could not parse  bucket list: %s", err.Error())
		return
	}

	if len(bucket_list) == 1 {
		ok = true
		id = bucket_list[0].ID
		return
	}
	if len(bucket_list) > 1 {
		err = fmt.Errorf("bucket name is not unique, got %d buckets", len(bucket_list))
		return
	}

	// no bucket found, this will return with ok == false
	return
}

func createBucket(name string) (bucket_id string, err error) {

	// curl -X POST 'localhost:8080/api/v1/objects?type=training-data&name=mybucket'  -H "Authorization: sage user:test"

	bucket_type := "training-data"

	client := &http.Client{}
	req, err := http.NewRequest("POST", fmt.Sprintf("%s/api/v1/objects?type=%s&name=%s&public=true", sage_storage_api, bucket_type, name), nil)
	if err != nil {
		return
	}
	// ...
	req.Header.Add("Authorization", "sage "+sage_storage_token)

	resp, err := client.Do(req)
	if err != nil {
		return
	}

	var b []byte
	b, err = io.ReadAll(resp.Body)
	if err != nil {
		if resp.StatusCode != 200 {
			err = fmt.Errorf("bucket creation failed, resp.StatusCode %d (and could not read response body)", resp.StatusCode)
			return
		}
		err = fmt.Errorf("bucket creation failed: %s", string(b[:]))
		return
	}

	bucket_object := SAGEBucket{}
	err = json.Unmarshal(b, &bucket_object)
	if err != nil {
		err = fmt.Errorf("could not parse bucket object: %s", err.Error())
		return
	}

	if bucket_object.Error != "" {
		err = fmt.Errorf("bucket creation failed: %s", bucket_object.Error)
		return
	}

	if resp.StatusCode != 200 {
		err = fmt.Errorf("bucket creation failed, resp.StatusCode %d (no error message retrieved)", resp.StatusCode)
		return
	}

	bucket_id = bucket_object.ID

	return
}
