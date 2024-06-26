package main

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"os"
	"net/http"
	"strconv"
	"strings"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type FileUploader interface {
	// TODO(sean) is this the right interface? maybe we can more closely match the s3 UploadInput?
	UploadFile(src, dst string, meta *MetaData) error
}

type PelicanFileUploaderConfig struct {
	Endpoint        string
	Bucket          string
}

type S3FileUploaderConfig struct {
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

type s3FileUploader struct {
	config  S3FileUploaderConfig
	session *session.Session
}

type pelicanFileUploader struct {
	config           PelicanFileUploaderConfig
	client           http.Client
	SignedJwtToken   string
}

func NewS3FileUploader(config S3FileUploaderConfig) (*s3FileUploader, error) {
	disableSSL := !strings.HasPrefix(config.Endpoint, "https")

	session, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
		Endpoint:         aws.String(config.Endpoint),
		Region:           aws.String(config.Region),
		DisableSSL:       aws.Bool(disableSSL),
		S3ForcePathStyle: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}

	return &s3FileUploader{
		config:  config,
		session: session,
	}, nil
}

//Initialize a new file uploader for Pelican by passing in a config, an initliaze JwtManager, and public key's id
func NewPelicanFileUploader(config PelicanFileUploaderConfig, jm JwtManager, ) (*pelicanFileUploader, error) {
	// Generate JWT token
	keyID := "1234" // Replace with actual key ID from JWKS
	token, err := jm.generateJwtToken(&keyID)
	if err != nil {
		return nil, fmt.Errorf("error generating JWT token: %v", err)
	}

	return &pelicanFileUploader{
		config:          config,
		client:          http.Client{},
		SignedJwtToken:  token,	
	}, nil
}

func (up *s3FileUploader) UploadFile(src, dst string, meta *MetaData) error {
	contentMD5, err := computeContentBase64MD5(src)
	if err != nil {
		return err
	}

	uploader := s3manager.NewUploader(up.session)
	if uploader == nil {
		return fmt.Errorf("could not create a new uploader")
	}

	stat, err := os.Stat(src)
	if err != nil {
		return err
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	// Upload the file to S3.
	// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/s3manager/#Uploader
	if _, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:     aws.String(up.config.Bucket),
		Key:        aws.String(dst),
		Body:       f,
		ContentMD5: aws.String(contentMD5),
		Metadata:   aws.StringMap(convertMetaToS3Metadata(meta)),
	}); err != nil {
		return fmt.Errorf("s3 uploader failed: %s", err.Error())
	}

	uploadFileMetrics(stat, meta)

	return nil
}

func (up *pelicanFileUploader) UploadFile(src, dst string, meta *MetaData) error {

	stat, err := os.Stat(src)
	if err != nil {
		return err
	}

	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()

	//Upload the file to Pelican
	req, err := http.NewRequest("PUT", up.config.Endpoint, f)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+string(up.SignedJwtToken))
	resp, err := up.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body: %v", err)
		}
		return fmt.Errorf("pelican uploader failed, non-OK HTTP status: %v \n response body: %s", resp.Status, body)
	}

	uploadFileMetrics(stat, meta)

	return nil
}

// update file metrics
func uploadFileMetrics(stat fs.FileInfo, meta *MetaData) {
	// TODO(sean) make these non-global variables
	// TODO(sean) think about splitting data vs meta files
	// TODO(sean) figure out how to get VSN metadata for metrics
	size := float64(stat.Size())
	node := meta.Meta["node"]
	uploadsProcessedTotal.Inc()
	uploadsProcessedBytes.Add(size)
	uploadsProcessedTotalByNode.WithLabelValues(node).Inc()
	uploadsProcessedBytesByNode.WithLabelValues(node).Add(size)
}

func convertMetaToS3Metadata(meta *MetaData) map[string]string {
	if meta == nil {
		return nil
	}
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
