package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// master的数据结构
type Filename string
type Location string
type ChunkHandle int

type master_meta struct {
	fnamespace []Filename // 文件名命名空间(持久化)
	// cnamespace      []ChunkHandle              // chunkHandle命名空间(持久化)
	fmc map[Filename][]ChunkHandle // 文件名映射chunkHandle(filename map chunk)(持久化)
	// cml             map[ChunkHandle][]Location // chunkHandle(chunk map location, location is ip address)(不持久化)
	// chunk_generator int                        // chunkHandle生成器
	// mu              sync.Mutex                 // 并发访问
}

var meta master_meta

// 初始化，从operation.log中读数据
func Init(meta *master_meta) {
	// operation.log格式：
	// 上载文件：[log] upload ${filename} shard ${chunkHandle[]} (${time})
	// 下载文件：[log] download ${filename} (${time})

	fmt.Println("world")
	file, err := os.Open("./log/operation.log")
	if err != nil {
		fmt.Println("Fail to open operation.log")
		os.Exit(-1)
	}
	reader := bufio.NewReader(file)
	for {
		line, _, err := reader.ReadLine()
		if err != nil || io.EOF == err {
			break
		} else {
			// 处理行
			elements := strings.Split(string(line), " ")
			if elements[1] == "upload" {
				len := len(elements)
				filename := elements[2]
				if _, i := meta.fmc[Filename(filename)]; i {
					continue
				}
				meta.fnamespace = append(meta.fnamespace, Filename(filename))
				chunks := []ChunkHandle{}
				for _, v := range elements[4 : len-1] {
					i, _ := strconv.Atoi(v)
					chunks = append(chunks, ChunkHandle(i))
				}
				fmt.Println("hello")
				meta.fmc[Filename(filename)] = chunks
			}
		}
	}
}

func main() {
	// 初始化metadata
	meta = master_meta{
		fnamespace: []Filename{},
		fmc:        make(map[Filename][]ChunkHandle),
	}
	Init(&meta)

	fmt.Println(meta)

	// http.ListenAndServe(":8080", nil)
}
