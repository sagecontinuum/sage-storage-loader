package main

type FileUploader interface {
	// TODO(sean) is this the right interface? maybe we can more closely match the s3 UploadInput?
	UploadFile(src, dst string, meta *MetaData) error
}
