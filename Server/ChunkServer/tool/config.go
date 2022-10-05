package tool

import (
	"encoding/json"
	"fmt"
	"os"
)

type CONFIG struct {
	Port string `json:"port"`
}

var Config CONFIG

func init() {
	// 这里不能写成../setting/setting.json
	// 因为程序 是在chunkServer.go开始运行的，以ChunkServer为当前目录
	file, err := os.Open("./setting/setting.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&Config)
	if err != nil {
		panic(err)
	}
	fmt.Println(Config)
}
