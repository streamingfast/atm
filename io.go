package atm

import (
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
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
	return ioutil.WriteFile(path, data, os.ModePerm)
}

func (f *FileIO) Read(path string) ([]byte, error) {
	return ioutil.ReadFile(path)
}

func (f *FileIO) Delete(path string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			zlog.Error("file delete: recovered from panic. if this happens too often disk might fill to capacity.", zap.Any("error", r))
			err = fmt.Errorf("recovered error while deleting file %s: %+v", path, r)
		}
	}()

	err = os.Remove(path)
	if err != nil {
		zlog.Error("deleting file", zap.Error(err), zap.String("path", path))
		return
	}

	return
}
