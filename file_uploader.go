package main

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type FileUploader interface {
	// TODO(sean) is this the right interface? maybe we can more closely match the s3 UploadInput?
	UploadFile(src, dst string, meta *MetaData) error
}

type S3Uploader struct{}

func (up *S3Uploader) UploadFile(src, dst string, meta *MetaData) error {
	contentMD5, err := computeContentBase64MD5(src)
	if err != nil {
		return err
	}

	// TODO(sean) decided if we want this optional check or not.
	if uploadExistsInS3(dst) {
		return nil
	}

	uploader := s3manager.NewUploader(newSession)

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#Uploader
	upi := &s3manager.UploadInput{
		Bucket:     aws.String(s3bucket),
		Key:        aws.String(dst),
		Body:       f,
		ContentMD5: aws.String(contentMD5),
		Metadata:   aws.StringMap(convertMetaToS3Metadata(meta)),
	}

	// Upload the file to S3.
	result, err := uploader.Upload(upi)
	if err != nil {
		return err
	}
	fmt.Printf("file uploaded to s3 at %s\n", result.Location)

	// TODO(sean) confirm file or content length can be read back out?
	// TODO(sean) the return string was left as missing before. why is this?
	return nil
}

func convertMetaToS3Metadata(meta *MetaData) map[string]string {
	m := map[string]string{
		"name": meta.Name,
		"ts":   strconv.FormatInt(meta.Timestamp.UnixNano(), 10),
	}
	if meta.Shasum != nil {
		m["shasum"] = *meta.Shasum
	}
	for k, v := range meta.Meta {
		m["meta."+k] = v
	}
	return m
}

func computeContentBase64MD5(name string) (string, error) {
	md5, err := computeContentMD5(name)
	if err != nil {
		return "", nil
	}
	return base64.StdEncoding.EncodeToString(md5), nil
}

func computeContentMD5(name string) ([]byte, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, nil
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func uploadExistsInS3(s3key string) bool {
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(s3bucket),
		Prefix:  aws.String(s3key),
		MaxKeys: aws.Int64(2),
	}
	result, err := svc.ListObjectsV2(input)
	return err == nil && len(result.Contents) == 2
}

type TestUploader struct{}

func (up *TestUploader) UploadFile(src, dst string, meta *MetaData) error {
	log.Printf("would upload file\nsrc: %s\ndst: %s\nmeta: %+v\n\n", src, dst, meta)
	return nil
}
