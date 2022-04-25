package main

import (
	"crypto/md5"
	"encoding/base64"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3FileUploader struct {
	Session *session.Session
	Bucket  string
}

func (up *S3FileUploader) UploadFile(src, dst string, meta *MetaData) error {
	contentMD5, err := computeContentBase64MD5(src)
	if err != nil {
		return err
	}

	uploader := s3manager.NewUploader(up.Session)

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	// Upload the file to S3.
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#Uploader
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:     aws.String(up.Bucket),
		Key:        aws.String(dst),
		Body:       f,
		ContentMD5: aws.String(contentMD5),
		Metadata:   aws.StringMap(convertMetaToS3Metadata(meta)),
	})
	if err != nil {
		return err
	}
	log.Printf("file uploaded to s3 at %s", result.Location)

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
