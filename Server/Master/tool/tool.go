package tool

import (
	"os"
)

func FileExist(filePath string) bool {
	_, err := os.Lstat(filePath)
	return !os.IsNotExist(err)
}
