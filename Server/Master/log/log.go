package log

import (
	stlog "log"
	"os"
)

var log *stlog.Logger

type fileLog string

// 为fileLog实现Write方法，具体是往文件中输出
// 于是fileLog就是writer接口
// 这样就可以通过fileLog来创建stLog
func (fl fileLog) Write(data []byte) (int, error) {
	f, err := os.OpenFile(string(fl), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return f.Write(data)
}

func Run(destination string) {
	log = stlog.New(fileLog(destination), "[log] ", stlog.LstdFlags)
}

func Write(data string) {
	log.Println(data)
}
