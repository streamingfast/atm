package atm

import (
	"io/fs"
	"io/ioutil"
	"os"
)

type CacheIO interface {
	Write(path string, data []byte) error
	Read(path string) ([]byte, error)
	Delete(path string) error
}

type fileIO struct{}

func (f *fileIO) Write(path string, data []byte) error {
	return ioutil.WriteFile(path, data, fs.ModePerm)
}

func (f *fileIO) Read(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func (f *fileIO) Delete(path string) error {
	return os.Remove(path)
}
