package main

import (
	"bufio"
	"bytes"
	"encoding/json"
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

	"github.com/log"
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

func metaInfo() {
	fmt.Println("==========metaInfo=============")
	fmt.Printf("*chunkHandle namespace*: %d\n", meta.chunk_generator)
	fmt.Println("-------------------------------")
	fmt.Println("*file namespace*:")
	for _, f := range meta.fnamespace {
		fmt.Println(f)
	}
	fmt.Println("-------------------------------")
	fmt.Println("*file map chunkHandles*:")
	for k, chunks := range meta.fmc {
		fmt.Print(k + " with ")
		for _, c := range chunks {
			fmt.Print(strconv.Itoa(int(c)) + " ")
		}
		fmt.Print("\n")
	}
	fmt.Println("-------------------------------")
	fmt.Println("*chunkHandle map location*:")
	for k, locations := range meta.cml {
		fmt.Printf("chunk %d :\n", int(k))
		for _, l := range locations {
			fmt.Println(l)
		}
	}
	fmt.Println("===============================")
}

// 初始化，从operation.log中读数据
func Init() {
	// operation.log格式：
	// 上载文件：[log] 2022/10/05 19:57:43 upload ${filename} shard ${chunkHandle[]}
	// 下载文件：[log] download ${filename} (${time})

	fmt.Println("world")
	file, err := os.OpenFile("./log/operation.log", os.O_CREATE|os.O_RDONLY|os.O_APPEND, 0600)
	// file, err := os.Open("./log/operation.log")
	if err != nil {
		fmt.Println("Fail to open operation.log")
		os.Exit(-1)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	for {
		line, _, err := reader.ReadLine()
		if err != nil || io.EOF == err {
			break
		} else {
			// 处理行
			elements := strings.Split(string(line), " ")
			if elements[3] == "<upload>" {
				// len := len(elements)
				filename := elements[4]
				// 同名文件，以后面的为准
				if _, i := meta.fmc[Filename(filename)]; !i {
					meta.fnamespace = append(meta.fnamespace, Filename(filename))
				}
				chunks := []ChunkHandle{}
				var max int
				for _, v := range elements[6:] {
					i, _ := strconv.Atoi(v)
					if i > max {
						max = i
					}
					chunks = append(chunks, ChunkHandle(i))
				}
				meta.fmc[Filename(filename)] = chunks
				meta.chunk_generator = max + 1
			}
		}
	}
	metaInfo()

	log.Run("./log/operation.log")
}

func sendFile(filePath, URL string) error {
	filename := filepath.Base(filePath)                                // 这里记载一下filepath.Base的用法(大概用处就是去掉前缀)
	bodyBuf := &bytes.Buffer{}                                         // 创建一个空的buf
	bodyWriter := multipart.NewWriter(bodyBuf)                         // 以bodyBuf为缓冲区，创建multipart.Writer
	fileWriter, err := bodyWriter.CreateFormFile("filename", filename) // 创建一个请求文件
	if err != nil {
		fmt.Println("Fail to create form file")
		return err
	}

	// 打开本地的要发送的文件
	file, err := os.Open(filePath) // 打开文件的时候不能使用filename
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
			fmt.Println("what")
			meta.mu.Unlock()
			continue
		}
		meta.cml[ChunkHandle(chunkHandle)] = append(meta.cml[ChunkHandle(chunkHandle)], Location(location))
		meta.mu.Unlock()
	}
	// fmt.Println(meta)
	// fmt.Println(r.Form)
	metaInfo()
}

func upload(w http.ResponseWriter, r *http.Request) {
	if len(meta.lnamespace) < K {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "no enough chunkServer")
		return
	}
	file, handler, err := r.FormFile("filename")
	if err != nil {
		// w.Write([]byte("can not open file")) // 不能使用这个来提示错误信息！否则报错Header被二次写入
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "can not open the file")
		return
	}

	num := math.Ceil(float64(handler.Size) / chunkSize)

	var i int64 = 1
	meta.mu.Lock()
	chunk_num := meta.chunk_generator
	defer meta.mu.Unlock()
	chunk_recorder := make(map[ChunkHandle][]Location)
	// 枚举chunk
	for ; i <= int64(num); i++ {
		file.Seek((i-1)*chunkSize, 0)
		var b []byte
		if chunkSize > int(handler.Size-(i-1)*chunkSize) {
			b = make([]byte, handler.Size-(i-1)*chunkSize)
		} else {
			b = make([]byte, chunkSize)
		}
		file.Read(b)

		// meta.mu.Lock() // 这个锁还有改进空间（太大了）
		// defer meta.mu.Unlock()

		Id := strconv.Itoa(chunk_num)
		// Id := strconv.Itoa(meta.chunk_generator)
		f, err := os.OpenFile("./files/"+Id+".chunk", os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		f.Write(b)
		f.Close()
		// 添加文件名到chunkhandle的映射
		// meta.fmc[Filename(handler.Filename)] = append(meta.fmc[Filename(handler.Filename)], ChunkHandle(meta.chunk_generator))

		// 将chunk发送给chunkserver后删除
		// 随机选取K个chunkserver
		rand.Seed(time.Now().Unix())
		m := make(map[int]bool)        // 检查是否已经发送给该chunk过了
		fail_recoder := make([]int, K) // 记录失败次数
		var count int
		for count < K {
			index := rand.Intn(len(meta.lnamespace))
			if _, i := m[index]; i {
				continue
			}
			err = sendFile("./files/"+Id+".chunk", string(meta.lnamespace[index])+"/chunk")
			if err != nil {
				// panic(err) // 这里直接就panic掉了，更好的方法应该是选取其他的fileserver。以后再写
				// 发送chunk失败，需要重新发送，容忍3次失败，否则直接报错
				if fail_recoder[count] < 3 {
					fmt.Println("fail!")
					fail_recoder[count] += 1
					continue
				} else {
					// 如果一个文件被分为3个chunk，前2个chunk发送成功，最后1个发送失败
					// 那么在master中不会记录有关这个文件的信息，即使有2个chunk被chunkserver存了
					// 在下载文件的时候，是检查不到这个文件的
					os.Remove("./files/" + Id + ".chunk")
					w.WriteHeader(http.StatusBadRequest)
					fmt.Fprint(w, "fail to send chunk to chunkServer")
					return
				}
			}
			// 记录chunkHandle到location的映射
			chunk_recorder[ChunkHandle(chunk_num)] = append(chunk_recorder[ChunkHandle(chunk_num)], meta.lnamespace[index])
			// meta.cml[ChunkHandle(meta.chunk_generator)] = append(meta.cml[ChunkHandle(meta.chunk_generator)], meta.lnamespace[index])
			count += 1

			os.Remove("./files/" + Id + ".chunk")
		}

		// meta.chunk_generator += 1
		chunk_num += 1
	}
	// 只有所有chunk都成功发送给了K个chunkServer才能更新meta
	meta.chunk_generator = chunk_num
	if _, i := meta.fmc[Filename(handler.Filename)]; !i {
		meta.fnamespace = append(meta.fnamespace, Filename(handler.Filename))
	}
	meta.fmc[Filename(handler.Filename)] = []ChunkHandle{} // 覆盖原来的fmc
	for k, v := range chunk_recorder {
		meta.fmc[Filename(handler.Filename)] = append(meta.fmc[Filename(handler.Filename)], k)
		meta.cml[k] = v
	}
	fmt.Println("aaaaa", chunk_recorder)
	info := "<upload> " + string(handler.Filename) + " <shard> "
	for k := range chunk_recorder {
		info += strconv.Itoa(int(k))
		info += " "
	}
	info = info[:len(info)-1]
	log.Write(info)

	metaInfo()

	file.Close()
}

// 下载文件，master只负责处理请求，将cml发送给client
// 再由client向chunkServer提出申请
func download(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	filename := r.Form["filename"][0] // 下载的文件名
	location := r.Form["location"][0] // 客户端地址
	fmt.Printf("filename is %s, location is %s\n", filename, location)

	if _, i := meta.fmc[Filename(filename)]; !i {
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "can not found file: %s\n", filename)
		return
	}

	// chunks := make([]string, len(meta.fmc[Filename(filename)])) // 这里不能用make，原因有待进一步分析
	chunks := []string{}
	for _, chunk := range meta.fmc[Filename(filename)] {
		chunks = append(chunks, strconv.Itoa(int(chunk)))
	}
	// ===test
	// fmt.Printf("test: chunks len is %d\n", len(chunks))
	// ===

	data := make(map[string][]string)
	data["chunks"] = chunks
	// ===test
	// fmt.Printf("test: chunks is %v\n", data["chunks"])
	// ===
	for _, chunk := range chunks {
		chunkInt, _ := strconv.Atoi(chunk)
		locations := meta.cml[ChunkHandle(chunkInt)]
		// ===test
		// fmt.Printf("test: locations is %v\n", locations)
		// fmt.Printf("test: locations len is %d\n", len(locations))
		// ===
		// locationsStr := make([]string, len(locations))
		locationsStr := []string{}
		for _, location := range locations {
			locationsStr = append(locationsStr, string(location))
		}
		data[chunk] = locationsStr
	}

	// ===test
	// fmt.Printf("test: len is %d\n", len(data["1"]))
	// fmt.Println(data["1"])
	// ===
	msg, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "can not marshal data")
		return
	}

	w.Write(msg)

	// response, err := http.PostForm(location, data)
	// if err != nil || response.StatusCode != http.StatusOK {
	// 	w.WriteHeader(http.StatusBadRequest)
	// 	fmt.Fprintf(w, "fail to return meta to client: %s", location)
	// 	return
	// }
}

func heartbeat() {
	for {
		meta.mu.Lock()
		lnamespace := meta.lnamespace
		meta.mu.Unlock()
		for _, location := range lnamespace {
			go func(location Location) {
				meta.mu.Lock()
				response, err := http.Get(string(location) + "/heartbeat")
				if err != nil || response.StatusCode != http.StatusOK {
					fmt.Printf("fail to contact with %s\n", string(location))
					// 将location去除
					for i, l := range meta.lnamespace {
						if l == location {
							fmt.Println("hi")
							meta.lnamespace = append(meta.lnamespace[:i], meta.lnamespace[i+1:]...)
							break
						}
					}
					// 将cml中的location去除，感觉很耗时。。。
					for chunk, locations := range meta.cml {
						for i, l := range locations {
							if l == location {
								meta.cml[chunk] = append(meta.cml[chunk][:i], meta.cml[chunk][i+1:]...)
								if len(meta.cml[chunk]) == 0 { // 这里似乎不能用meta.cml[chunk] == nil来判断
									fmt.Println("删除")
									delete(meta.cml, chunk)
								}
							}
						}
					}
					fmt.Println(meta.lnamespace)
					fmt.Println(meta.cml)
				} else {
					fmt.Printf("%s is fine\n", string(location))
					fmt.Println(meta.cml)
				}
				meta.mu.Unlock()
			}(location)
		}
		time.Sleep(time.Second)
	}
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

	// 处理下载请求
	http.HandleFunc("/download", download)

	// 发送心跳检查
	go heartbeat()

	// fmt.Println(meta)

	http.ListenAndServe(":8080", nil)
}
