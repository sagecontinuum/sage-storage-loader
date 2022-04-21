package main

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type FileUploader interface {
	// TODO(sean) is this the right interface? maybe we can more closely match the s3 UploadInput?
	UploadFile(src, dst string, meta map[string]string) error
}

type MockUploader struct {
	Error   error
	uploads map[string]string
}

func NewMockUploader() *MockUploader {
	return &MockUploader{
		uploads: make(map[string]string),
	}
}

func (up *MockUploader) UploadFile(src, dst string, meta map[string]string) error {
	if up.Error != nil {
		return up.Error
	}
	if dst2, ok := up.uploads[src]; ok && dst != dst2 {
		return fmt.Errorf("file uploaded to multiple destinations")
	}
	up.uploads[src] = dst
	return nil
}

func (up *MockUploader) WasUploaded(src, dst string) bool {
	dst2, ok := up.uploads[src]
	return ok && dst == dst2
}

type S3Uploader struct{}

func (up *S3Uploader) UploadFile(src, dst string, meta map[string]string) error {
	contentMD5, err := computeContentBase64MD5(src)
	if err != nil {
		return err
	}

	// optional - check if file already exists or if content hash matches
	// if uploadExistsInS3(filepath.Join(s3path, targetNameData)) {
	// 	fmt.Println("Files already exist in S3")
	// 	return nil
	// }

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
		Metadata:   aws.StringMap(meta),
	}

	// Upload the file to S3.
	result, err := uploader.Upload(upi)
	if err != nil {
		return err
	}
	fmt.Printf("file uploaded to, %s\n", result.Location)

	// TODO(sean) confirm file or content length can be read back out?
	// TODO(sean) the return string was left as missing before. why is this?
	return nil
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
