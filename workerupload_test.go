package main

import (
	"testing"
)

func TestComputeContentBase64MD5(t *testing.T) {
	s, err := computeContentBase64MD5("testcontent")
	if err != nil {
		t.Fatal(err)
	}
	want := "3yl0d/FXREMf5SYiTD0sOg=="
	if s != want {
		t.Fatalf("content md5 does not match. want: %q got: %q", want, s)
	}
}
