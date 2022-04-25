package main

import "log"

type LogFileUploader struct{}

func (up *LogFileUploader) UploadFile(src, dst string, meta *MetaData) error {
	log.Printf("would upload file\nsrc: %s\ndst: %s\nmeta: %+v\n\n", src, dst, meta)
	return nil
}
