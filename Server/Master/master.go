package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/log"
	"github.com/tool"
)

const chunkSize = 1024 * 1024
const K = 2 // 表示chunk需要被复制并发送到不同chunkServer的份数，chunkServer数量需不小于K

var netListen net.Listener

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

type MetaWithoutLock struct {
	FNamespace []Filename                 `json:"fnamespace"`
	LNamespace []Location                 `json:"lnamespace"`
	FMC        map[Filename][]ChunkHandle `json:"fml"`
	CML        map[ChunkHandle][]Location `json:"cml"`
}

type JsonResult struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
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
	fmt.Println("*location namespace*:")
	for _, l := range meta.lnamespace {
		fmt.Println(l)
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

// Init 初始化，从operation.log中读数据
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
				// var max int
				for _, v := range elements[6:] {
					i, _ := strconv.Atoi(v)
					if i >= meta.chunk_generator {
						meta.chunk_generator = i + 1
					}
					// if i > max {
					// 	max = i
					// }
					chunks = append(chunks, ChunkHandle(i))
				}
				meta.fmc[Filename(filename)] = chunks
				// meta.chunk_generator = max + 1
			} else if elements[3] == "<removeTmp>" {
				for _, v := range elements[6:] {
					i, _ := strconv.Atoi(v)
					if i >= meta.chunk_generator {
						meta.chunk_generator = i + 1
					}
				}
			}
		}
	}

	// 在临时文件中找到最大的，临时文件中的chunk都已经持久化了，但是不一定被generator记录
	files, err := ioutil.ReadDir("./files")
	if err != nil {
		fmt.Println("can not open directory: files")
		os.Exit(-1)
	}
	for _, file := range files {
		f, err := os.Open("./files/" + file.Name())
		if err != nil {
			fmt.Printf("can not open file: %s\n", file.Name())
			os.Exit(-1)
		}
		content, _ := io.ReadAll(f)
		chunks := strings.Split(string(content), " ")
		for _, chunk := range chunks {
			if chunkInt, _ := strconv.Atoi(chunk); chunkInt >= meta.chunk_generator {
				meta.chunk_generator = chunkInt + 1
			}
		}
	}
	metaInfo()

	log.Run("./log/operation.log")

	// 开启tcp服务
	netListen, err = net.Listen("tcp", "localhost:1024")
	if err != nil {
		fmt.Println(err)
		return
	}
	// defer netListen.Close()

	fmt.Println("waiting for clients ...")
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
		meta.cml[ChunkHandle(chunkHandle)] = append(meta.cml[ChunkHandle(chunkHandle)], Location(location))
		meta.mu.Unlock()
	}
	// fmt.Println(meta)
	// fmt.Println(r.Form)
	metaInfo()
}

// 对于给定的chunkHandle(string)，到随机的chunkServer中找到内容并返回
func getChunk(chunk string) ([]byte, error) {
	chunkInt, _ := strconv.Atoi(chunk)
	chunkHandle := ChunkHandle(chunkInt)
	var faultTime int
	m := make(map[int]bool)
	for faultTime < 3 {
		meta.mu.Lock()
		index := rand.Intn(len(meta.cml[chunkHandle]))
		meta.mu.Unlock()
		if m[index] {
			continue
		}

		// 向chunkServer发送获取chunk的请求
		meta.mu.Lock()
		location := meta.cml[chunkHandle][index]
		meta.mu.Unlock()
		params := url.Values{}
		parseURL, err := url.Parse(string(location) + "/chunk")
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		params.Set("chunkHandle", strconv.Itoa(int(chunkHandle)))
		parseURL.RawQuery = params.Encode()
		urlPathWithParams := parseURL.String()
		fmt.Printf("check: url is %s\n", urlPathWithParams)
		response, err := http.Get(urlPathWithParams)
		if err != nil {
			faultTime += 1
			fmt.Printf("fail to check for the %d time: %v\n", faultTime, err)
			continue
		}
		defer response.Body.Close()

		if response.StatusCode != http.StatusOK {
			faultTime += 1
			fmt.Printf("fail to check for the %d time: %v\n", faultTime, response.Body)
			continue
		}

		msg, err := io.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
			return nil, err
		} else {
			return msg, nil
		}
	}
	return nil, errors.New("can not get chunk")
}

// 通过TCP，向随机的chunkServer发送chunk
func send(chunkNum ChunkHandle, chunkFileName string, conn net.Conn) {
	rand.Seed(time.Now().Unix())
	m := make(map[int]bool)        // 检查是否已经发送给该chunk过了
	fail_recoder := make([]int, K) // 记录失败次数
	var count int
	for count < K {
		index := rand.Intn(len(meta.lnamespace))
		if _, i := m[index]; i {
			continue
		}
		err := sendFile("./files/"+chunkFileName, string(meta.lnamespace[index])+"/chunk")
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
				os.Remove("./files/" + chunkFileName)

				msg := "[error] 发送" + chunkFileName + "失败"
				conn.Write([]byte(msg))
				return
			}
		}
		// 记录chunkHandle到location的映射
		// chunk_recorder[ChunkHandle(*chunkNum)] = append(chunk_recorder[ChunkHandle(*chunkNum)], meta.lnamespace[index])
		meta.cml[ChunkHandle(chunkNum)] = append(meta.cml[ChunkHandle(chunkNum)], meta.lnamespace[index])

		count += 1
		m[index] = true
	}
}

func handleConnection(conn net.Conn) {
	var msg string
	var fileName string
	// var fileSize int64
	// 获取文件名
	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err)
		msg = fmt.Sprintf("can not read filename: %v", err)
		conn.Write([]byte(msg))
		return
	} else {
		fmt.Printf("filename from client: %s\n", string(buf))
		conn.Write([]byte("ok"))
	}
	fileName = string(buf[:n])

	tmpFilename := fileName + ".tmp"
	tmpFilePath := "./files/" + tmpFilename
	hasTmpFile := tool.FileExist(tmpFilePath)
	fmt.Printf("%s, hasTmpFile is %v!!!!!!\n", tmpFilePath, hasTmpFile)
	chunkNumRecoder := []int{} // 记录chunkHandle
	var tmpFile *os.File
	var tmpFileChanged bool
	// 解释一下tmpFileChanged的作用：
	// 这个变量名没有取好，它是用来判断本次上传的文件和以往被中断的文件(的已上传部分)是否相同
	// 若相同，则不用从头开始传，继续从conn中读取内容即可
	// 若不同，则需要从头开始传。由于conn中部分内容已经被读掉了，所以fileBuf用来保存这部分内容

	fileBuf := bytes.Buffer{} // 读取文件
	chunkBuf := make([]byte, chunkSize)

	if hasTmpFile { // 是一个断点文件
		msg = "[info] 发现传输中断的文件：" + tmpFilename
		conn.Write([]byte(msg))

		tmpFile, err = os.OpenFile(tmpFilePath, os.O_APPEND, 0600)
		if err != nil {
			msg = "[wrong] 无法打开临时文件： %s" + tmpFilename
			conn.Write([]byte(msg))
			return
		}

		res, _ := io.ReadAll(tmpFile)
		if string(res) == "" {
			goto StartUploading
		}
		chunks := strings.Split(string(res), " ")

		// 验证已经写入的chunk与将要写入的文件内容是否一致
		//var same bool = true
		for _, chunk := range chunks {
			content, err := getChunk(chunk)
			if err != nil {
				msg = "[wrong] 获取chunk文件" + chunk + ".chunk失败"
				conn.Write([]byte(msg))
				return
			}

			// 检查
			n, _ := conn.Read(chunkBuf)
			fileBuf.Write(chunkBuf[:n]) // 加入到文件中
			if !bytes.Equal(chunkBuf, content) {
				tmpFileChanged = true
				//same = false
				break
			}
		}
		if tmpFileChanged { // 文件有变化
			msg = "[info] 检测到文件内容发生变化，正在替换文件..."
			conn.Write([]byte(msg))

			tmpFile.Close()
			os.Remove(tmpFilePath)

			tmpFile, err = os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDWR, 0600)
			fmt.Println("here!")
			if err != nil {
				msg = "[wrong] 无法创建临时文件：" + tmpFilename + ".tmp"
				conn.Write([]byte(msg))
				return
			}

			info := "<removeTmp> " + string(tmpFilename) + " <shard> "
			for _, chunk := range chunks {
				info += chunk + " "
			}
			info = info[:len(info)-1]
			log.Write(info)
		} else { // 文件无变化
			msg = "[info] 文件检查无误，正在续传文件..."
			conn.Write([]byte(msg))

			for _, chunk := range chunks {
				chunkInt, _ := strconv.Atoi(chunk)
				chunkNumRecoder = append(chunkNumRecoder, chunkInt)
			}
		}
	} else { // 一个正常的文件
		msg = "[info] 正在上传..."
		conn.Write([]byte(msg))

		tmpFile, err = os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDWR, 0600)
		fmt.Println("here there!!!")
		if err != nil {
			msg = "[wrong] 无法创建临时文件：" + tmpFilename + ".tmp"
			conn.Write([]byte(msg))
			return
		}
	}

StartUploading:
	// 开始传
	chunkNum := &meta.chunk_generator
	var first bool = true
	var ok bool
	// 先传fileBuf中的部分（检查无误的文件直接传conn中的就行）
	for {
		if ok {
			break
		}
		chunkBuf := make([]byte, chunkSize)
		if tmpFileChanged {
			n, err = fileBuf.Read(chunkBuf)
		} else {
			n, err = conn.Read(chunkBuf)
		}
		if err != nil {
			if err == io.EOF && !tmpFileChanged {
				tmpFileChanged = true
				continue
			} else {
				msg = "[error] 无法读取要传输的文件"
				conn.Write([]byte(msg))
				return
			}
		}
		if string(chunkBuf[n-6:n]) == "finish" {
			chunkBuf = chunkBuf[:n-6]
			ok = true
		}

		meta.mu.Lock()
		// 在master创建chunk临时文件
		Id := strconv.Itoa(*chunkNum)
		chunkFileName := Id + ".chunk"
		f, err := os.OpenFile("./files/"+chunkFileName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
		if err != nil {
			msg = "[error] 无法创建chunk临时文件" + chunkFileName
			conn.Write([]byte(msg))
			meta.mu.Unlock()
			return
		}
		f.Write(chunkBuf)
		f.Close()

		// 发送该chunk临时文件，需要检查chunkServer数量和K
		if len(meta.lnamespace) >= K {
			send(ChunkHandle(*chunkNum), chunkFileName, conn)
		} else {
			msg = "[error] 文件服务器数量不足，请联系管理员添加"
			conn.Write([]byte(msg))
			meta.mu.Unlock()
			return
		}
		// 修改元数据
		chunkNumRecoder = append(chunkNumRecoder, *chunkNum)
		if first {
			tmpFile.Write([]byte(strconv.Itoa(*chunkNum)))
			first = false
		} else {
			tmpFile.Write([]byte(" " + strconv.Itoa(*chunkNum)))
		}
		// meta.chunk_generator += 1
		os.Remove("./files/" + Id + ".chunk")
		*chunkNum += 1

		// ===test 人为打断
		//if !first {
		//	return
		//}
		// ===
		meta.mu.Unlock()
	}

	meta.mu.Lock()
	_, exist := meta.fmc[Filename(fileName)]

	meta.fmc[Filename(fileName)] = []ChunkHandle{}
	for _, v := range chunkNumRecoder {
		meta.fmc[Filename(fileName)] = append(meta.fmc[Filename(fileName)], ChunkHandle(v))
	}

	if !exist {
		meta.fnamespace = append(meta.fnamespace, Filename(fileName))
	}

	tmpFile.Close() // 必须要先关闭文件才能删除文件
	os.Remove(tmpFilePath)

	info := "<upload> " + string(fileName) + " <shard> "
	for _, k := range chunkNumRecoder {
		info += strconv.Itoa(int(k))
		info += " "
	}
	info = info[:len(info)-1]
	log.Write(info)

	conn.Write([]byte("end"))

	metaInfo()
	meta.mu.Unlock()
}

func tcpUpload() {
	for {
		// fmt.Println("你干嘛")
		conn, err := netListen.Accept() //监听接收
		if err != nil {
			continue //如果发生错误，继续下一个循环。
		}

		fmt.Println("tcp connect success")
		go handleConnection(conn)
	}
}

// 下载文件，master只负责处理请求，将cml发送给client
// 再由client向chunkServer提出申请
func download(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	filename := r.Form["filename"][0] // 下载的文件名
	location := r.Form["location"][0] // 客户端地址（这似乎没用）
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

	data := make(map[string][]string)
	data["chunks"] = chunks
	for _, chunk := range chunks {
		chunkInt, _ := strconv.Atoi(chunk)
		locations := meta.cml[ChunkHandle(chunkInt)]
		locationsStr := []string{}
		for _, location := range locations {
			locationsStr = append(locationsStr, string(location))
		}
		data[chunk] = locationsStr
	}

	msg, err := json.Marshal(data)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "can not marshal data")
		return
	}

	w.Write(msg)

	log.Write("<download> " + string(filename))
}

func list(w http.ResponseWriter, r *http.Request) {
	data, _ := json.Marshal(meta.fnamespace)
	w.Write(data)
}

// check请求，用于返回meta中的部分数据给客户端
// 要注意对某个结构体序列化时，字段的首字母要大写
func check(w http.ResponseWriter, r *http.Request) {
	meta.mu.Lock()
	info := MetaWithoutLock{
		FNamespace: meta.fnamespace,
		LNamespace: meta.lnamespace,
		FMC:        meta.fmc,
		CML:        meta.cml,
	}
	meta.mu.Unlock()
	msg, err := json.Marshal(info)
	if err != nil {
		fmt.Println("cmn")
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("服务器序列化数据失败"))
		return
	}
	w.Write(msg)
	log.Write("<check>")
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
					// metaInfo()
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
					metaInfo()
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

	go tcpUpload()

	// chunkserver汇报
	// 请求格式：post
	// location: string
	// chunks: []int
	http.HandleFunc("/report", report)

	// 处理下载请求
	http.HandleFunc("/download", download)

	// 处理list请求
	http.HandleFunc("/list", list)

	http.HandleFunc("/check", check)

	// 发送心跳检查
	go heartbeat()

	metaInfo()
	http.ListenAndServe(":8080", nil)
}
