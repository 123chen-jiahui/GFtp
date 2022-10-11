package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tool"
)

const chunkSize = 1024 * 1024

// StrInterface 将interface转换为string
func StrInterface(i interface{}) string {
	s := fmt.Sprintf("%v", i)
	return s
}

var action = flag.String("action", "", "upload or download")
var p = flag.String("path", "", "the path of file uploaded or the saved")
var f = flag.String("file", "", "the file you want to download from server")

func tip(prefix, content string) {
	fmt.Println("[" + prefix + "] " + content)
}

func tcpUpload(filePath string) {
	server := tool.Config.MasterIpPort
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		tip("error", err.Error())
		return
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		tip("error", err.Error())
		return
	}
	tip("info", "TCP连接建立成功")

	buffer := make([]byte, 2048)

	// 发送文件名
	fileName := filepath.Base(filePath)
	conn.Write([]byte(fileName))
	n, _ := conn.Read(buffer)
	if string(buffer[:n]) != "ok" {
		tip("error", "发送文件名失败")
		return
	}

	// 此处信道作用：master发出任何error信号时，停止client的上传文件操作
	// 一没必要；二如果文件较大，会阻塞缓冲区
	ch := make(chan int)
	var wg sync.WaitGroup
	wg.Add(1)
	go func(conn net.Conn, ch chan int) {
		buffer := make([]byte, 100)
		for {
			n, err = conn.Read(buffer)
			if err != nil {
				tip("error", "读取服务器返回信息出错："+err.Error())
				os.Exit(-1)
			}
			if string(buffer[:n]) == "end" {
				tip("info", "文件上传成功")
				break
			} else {
				fmt.Println(string(buffer[:n]))
				msgList := strings.Split(string(buffer[:n]), " ")
				if msgList[0] == "[error]" {
					ch <- 1
					break
				}
			}
		}
		wg.Done()
	}(conn, ch)

	// conn.Write([]byte("ok"))
	// 发送文件内容
	file, err := os.Open(filePath)
	if err != nil {
		tip("error", "打开文件"+filePath+"失败")
		return
	}
	defer file.Close()
Loop:
	for {
		buf := make([]byte, chunkSize)
		n, err := file.Read(buf)
		if err != nil && io.EOF == err {
			conn.Write([]byte("finish"))
			break
		}
		select {
		case <-ch:
			break Loop
		default:
			conn.Write(buf[:n])
		}
	}

	wg.Wait()
}

// 用于批量移除指定目录中的指定后缀文件
func removeFilesWithSuffix(prefix []string, suffix, dir string) {
	for _, p := range prefix {
		os.Remove(dir + "/" + p + suffix)
	}
}

// 向指定chunkServer获取指定的chunk
func getChunk(location string, chunk string) ([]byte, error) {
	response, err := http.PostForm(location+"/download", url.Values{
		"chunk": []string{chunk},
	})
	if err != nil {
		fmt.Println(err)
		return nil, errors.New("向chunkServer发送HTTP请求失败")
	}
	responseBody, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("http请求错误，状态码：%v，响应：%v", response.StatusCode, responseBody)
		return nil, errors.New(msg)
	}
	return responseBody, nil
}

// 该函数发送用于下载服务器上的文件
// 向master发送文件名filename
// master的返回有两部分：需要下载的chunk集合chunks，以及对应的chunkServer地址locations
// 然后再向chunkServer发送请求获取chunk
// 最后将结果拼在一起得到最终的文件
// 改进意见：
// 1、这里可以改成get请求（小事）
func download(filename string, filepath string) {
	response, err := http.PostForm(tool.Config.MasterURL+"download", url.Values{
		"filename": []string{filename},
		"location": []string{tool.Location},
	})
	// 不知道没有成功发送请求是不是错误...
	// 答：不是错误
	if err != nil {
		tip("error", "发送下载请求失败")
		return
	}
	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		msg := fmt.Sprintf("http请求错误，状态码：%v，响应：%v", response.StatusCode, string(responseBody))
		tip("error", msg)
		return
	}

	msg, _ := io.ReadAll(response.Body)

	// res是string到interface的映射
	res := make(map[string]interface{})
	err = json.Unmarshal(msg, &res)
	if err != nil {
		tip("error", "反序列化数据出错")
		return
	}

	// 接下来向各个chunkServer发送请求
	chunksInterface := res["chunks"].([]interface{})

	tmpDir := os.TempDir()
	fmt.Println(tmpDir)
	tmpFileName := filename + ".tmp"
	tmpFilePath := tmpDir + "/" + tmpFileName
	hasTmpFile := tool.FileExist(tmpFilePath)
	var tmpFile *os.File            // 临时文件
	var checkOk = true              // 在临时文件存在的基础上，检查是否无误
	var ptr int                     // chunksInterface的指针
	bytesChecked := &bytes.Buffer{} // 用于存放已经检查过的chunk的内容

	rand.Seed(time.Now().Unix())

	if hasTmpFile {
		tip("info", "检测到下载中断的文件："+filename)
		tmpFile, err = os.OpenFile(tmpFilePath, os.O_APPEND, 0600) // 追加权限
		if err != nil {
			tip("error", "临时文件打开失败")
			return
		}
		tmpFileContent, err := io.ReadAll(tmpFile)
		if err != nil {
			tip("error", "读取临时文件出错")
			return
		}
		if string(tmpFileContent) == "" {
			goto ContinueDownload
		}
		// 临时文件中的chunk
		tmpFileChunksStr := strings.Split(string(tmpFileContent), " ")
		// 检查内容是否发送变化
		for _, tmpFileChunkStr := range tmpFileChunksStr {
			// 检查是否在chunksInterface里面
			if _, i := res[tmpFileChunkStr]; i {
				locationsInterface := res[tmpFileChunkStr].([]interface{})
				index := rand.Intn(len(locationsInterface))
				locationStr := StrInterface(locationsInterface[index])

				contentFromRemote, err := getChunk(locationStr, tmpFileChunkStr)
				if err != nil {
					errorMessage := fmt.Sprint(err)
					tip("error", errorMessage)
					return
				}

				bytesChecked.Write(contentFromRemote)

				// 来自本地的chunk内容
				contentFromLocal, err := os.ReadFile(tmpDir + "/" + tmpFileChunkStr + ".chunk")
				if err != nil {
					tip("error", "无法打开chunk文件："+tmpFileChunkStr+".chunk")
					return
				}
				if !bytes.Equal(contentFromLocal, contentFromRemote) { // 内容检测不通过
					checkOk = false
					break
				}
			} else { // chunkHandle检测不通过
				checkOk = false
				break
			}
			ptr += 1
		}
		if !checkOk { // 如果检测不通过，则重新创建文件
			tip("info", "检测到文件内容发生变化，正在替换文件...")

			removeFilesWithSuffix(tmpFileChunksStr, ".chunk", tmpDir)
			tmpFile.Close()
			os.Remove(tmpFilePath)

			tmpFile, err = os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDWR, 0600)
			if err != nil {
				tip("error", "无法创建临时文件："+tmpFileName)
				return
			}
		} else {
			tip("info", "文件检查无误，正在继续下载")
		}
	} else { // 这是一个正常的下载过程
		tip("info", "正在下载文件")

		tmpFile, err = os.OpenFile(tmpFilePath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			tip("error", "无法创建临时文件："+tmpFileName)
			return
		}
	}

ContinueDownload:
	// 获取剩余为获取的chunk
	bytesLeft := bytes.Buffer{}
	var strOfChunksInterface []string // 保存chunksInterface的string类型拷贝
	for ptr < len(chunksInterface) {
		//if ptr > 3 {
		//	return
		//}
		chunkStr := StrInterface(chunksInterface[ptr])
		strOfChunksInterface = append(strOfChunksInterface, chunkStr)
		locationsInterface := res[chunkStr].([]interface{})
		index := rand.Intn(len(locationsInterface))
		locationStr := StrInterface(locationsInterface[index])
		content, err := getChunk(locationStr, chunkStr)
		if err != nil {
			errorMessage := fmt.Sprint(err)
			tip("error", errorMessage)
			return
		}
		// 先要将获取的chunk保存到临时文件中
		tmpChunkFile, err := os.OpenFile(tmpDir+"/"+chunkStr+".chunk", os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			tip("error", "无法创建临时chunk文件")
			return
		}
		tmpChunkFile.Write(content)
		tmpChunkFile.Close()
		// 并将结果写入临时文件
		if ptr == 0 {
			tmpFile.Write([]byte(chunkStr))
		} else {
			tmpFile.Write([]byte(" " + chunkStr))
		}
		// 写入内存
		bytesLeft.Write(content)
		ptr += 1
	}
	tmpFile.Close()

	// 整合
	bytesAll := bytes.Buffer{}
	bytesAll.Write(bytesChecked.Bytes())
	bytesAll.Write(bytesLeft.Bytes())

	// 生成最终的文件
	targetFile, err := os.OpenFile(path.Join(filepath, filename), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		tip("error", "创建文件失败")
		return
	}
	targetFile.Write(bytesAll.Bytes())
	targetFile.Close()
	tip("info", "下载文件成功")
	// 删除临时文件
	removeFilesWithSuffix(strOfChunksInterface, ".chunk", tmpDir)
	os.Remove(tmpFilePath)
	return
}

// 向master询问文件列表
func list() {
	response, err := http.Get(tool.Config.MasterURL + "list")
	if err != nil {
		msg := fmt.Sprintf("无法与服务器通信：%v", err)
		tip("error", msg)
		return
	}
	if response.StatusCode != http.StatusOK {
		responseBody, _ := io.ReadAll(response.Body)
		msg := fmt.Sprintf("http请求错误，状态码：%v，响应：%v", response.StatusCode, string(responseBody))
		tip("error", msg)
		return
	}

	msg, _ := io.ReadAll(response.Body)

	var res []string
	err = json.Unmarshal(msg, &res)
	if err != nil {
		msg := fmt.Sprintf("数据反序列化出错：%v", err)
		tip("error", msg)
		return
	}
	for i, v := range res {
		fmt.Printf("%d. %s\n", i+1, v)
	}
}

func main() {
	flag.Parse()

	switch *action {
	case "upload":
		tcpUpload(*p)
	case "list":
		list()
	case "download":
		download(*f, *p)
	default:
		fmt.Printf("unknown action: %s\n", *action)
	}
}
