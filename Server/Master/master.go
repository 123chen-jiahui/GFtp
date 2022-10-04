package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const chunkSize = 1024 * 1024
const K = 1

// master的数据结构
type Filename string
type Location string
type ChunkHandle int

type master_meta struct {
	fnamespace []Filename // 文件名命名空间(持久化)
	// cnamespace      []ChunkHandle              // chunkHandle命名空间(持久化)
	lnamespace      []Location                 // chunkserver位置（ip地址）命名空间
	fmc             map[Filename][]ChunkHandle // 文件名映射chunkHandle(filename map chunk)(持久化)
	cml             map[ChunkHandle][]Location // chunkHandle(chunk map location, location is ip address)(不持久化)
	chunk_generator int                        // chunkHandle生成器
	mu              sync.Mutex                 // 并发访问
}

var meta = master_meta{
	fnamespace: []Filename{},
	fmc:        make(map[Filename][]ChunkHandle),
	cml:        make(map[ChunkHandle][]Location),
}

// 初始化，从operation.log中读数据
func Init() {
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

func sendFile(filePath, URL string) error {
	filename := filepath.Base(filePath)                                // 这里记载一下filepath.Base的用法
	bodyBuf := &bytes.Buffer{}                                         // 创建一个空的buf
	bodyWriter := multipart.NewWriter(bodyBuf)                         // 以bodyBuf为缓冲区，创建multipart.Writer
	fileWriter, err := bodyWriter.CreateFormFile("filename", filename) // 创建一个请求文件
	if err != nil {
		fmt.Println("Fail to create form file")
		return err
	}

	// 打开本地的要发送的文件
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Fail to open file: %s\n", filePath)
		return err
	}
	defer file.Close()

	_, err = io.Copy(fileWriter, file)
	if err != nil {
		return err
	}
	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()
	response, err := http.Post(URL, contentType, bodyBuf)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		fmt.Printf("Fail to send chunk: %s\n", filename)
		return errors.New("fail to send chunk")
	}

	return nil
}

func report(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	location := r.Form["location"][0]
	// 将location添加入命名空间，这里假设location是不重复的
	meta.mu.Lock()
	meta.lnamespace = append(meta.lnamespace, Location(location))
	meta.mu.Unlock()

	for _, v := range r.Form["chunks"] {
		chunkHandle, _ := strconv.Atoi(v)
		meta.mu.Lock()
		if _, i := meta.cml[ChunkHandle(chunkHandle)]; i {
			meta.mu.Unlock()
			continue
		}
		meta.cml[ChunkHandle(chunkHandle)] = append(meta.cml[ChunkHandle(chunkHandle)], Location(location))
		meta.mu.Unlock()
	}
	// fmt.Println(meta)
	// fmt.Println(r.Form)
}

func upload(w http.ResponseWriter, r *http.Request) {
	file, handler, err := r.FormFile("filename")
	if err != nil {
		fmt.Println(err)
		return
	}

	num := math.Ceil(float64(handler.Size) / chunkSize)

	var i int64 = 1
	for ; i <= int64(num); i++ {
		file.Seek((i-1)*chunkSize, 0)
		var b []byte
		if chunkSize > int(handler.Size-(i-1)*chunkSize) {
			b = make([]byte, handler.Size-(i-1)*chunkSize)
		} else {
			b = make([]byte, chunkSize)
		}
		file.Read(b)

		Id := strconv.Itoa(meta.chunk_generator)
		f, err := os.OpenFile("./files/"+Id+".chunk", os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			panic(err)
		}
		f.Write(b)
		f.Close()
		// 添加文件名到chunkhandle的映射
		meta.fmc[Filename(handler.Filename)] = append(meta.fmc[Filename(handler.Filename)], ChunkHandle(meta.chunk_generator))

		// 将chunk发送给chunkserver后删除
		// 随机选取K个chunkserver
		rand.Seed(time.Now().Unix())
		m := make(map[int]bool)
		var count int
		for count < K {
			index := rand.Intn(len(meta.lnamespace))
			if _, i := m[index]; i {
				continue
			}
			err = sendFile("./files/"+Id+".chunk", string(meta.lnamespace[index]))
			if err != nil {
				panic(err) // 这里直接就panic掉了，更好的方法应该是选取其他的fileserver。以后再写
			}
			// 添加chunkHandle到location的映射
			meta.cml[ChunkHandle(meta.chunk_generator)] = append(meta.cml[ChunkHandle(meta.chunk_generator)], meta.lnamespace[index])
			count += 1

			os.Remove("./files/" + Id + ".chunk")
		}
		meta.chunk_generator += 1
	}
	file.Close()
}

func main() {
	// 初始化metadata
	Init()

	// chunkserver汇报
	// 请求格式：post
	// location: string
	// chunks: []int
	http.HandleFunc("/report", report)

	// 处理上载请求
	http.HandleFunc("/upload", upload)

	// fmt.Println(meta)

	http.ListenAndServe(":8080", nil)
}
