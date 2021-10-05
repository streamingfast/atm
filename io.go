package atm

import (
	"os"
)

type CacheIO interface {
	Write(path string, data []byte) error
	Read(path string) ([]byte, error)
	Delete(path string) error
}

type FileIO struct{}

func NewFileIO() *FileIO {
	return &FileIO{}
}

func (f *FileIO) Write(path string, data []byte) error {
	return os.WriteFile(path, data, os.ModePerm)
}

func (f *FileIO) Read(path string) ([]byte, error) {
	return os.ReadFile(path)
}

func (f *FileIO) Delete(path string) error {
	return os.Remove(path)
}
