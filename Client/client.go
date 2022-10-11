package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
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
	"strings"
	"sync"
	"time"

	"github.com/tool"
)

const chunkSize = 1024 * 1024

func StrInterface(i interface{}) string {
	s := fmt.Sprintf("%v", i)
	return s
}

var action = flag.String("action", "", "upload or download")
var path = flag.String("path", "", "the path of file uploaded or the saved")

func tip(prefix, content string) {
	fmt.Println("[" + prefix + "] " + content)
}

func upload(filePath string) error {
	filename := filepath.Base(filePath)

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)
	fileWriter, err := bodyWriter.CreateFormFile("filename", filename)
	if err != nil {
		fmt.Println("fail to create form file")
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("fail to open file: %s\n", filePath)
		return err
	}
	defer file.Close()

	_, err = io.Copy(fileWriter, file)
	if err != nil {
		fmt.Println("fail to copy content from origin file to form file")
		return err
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()
	response, err := http.Post(tool.Config.MasterURL+"upload", contentType, bodyBuf)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		fmt.Printf("fail to upload file: %s\n", filePath)
		return errors.New("fail to upload file")
	}

	return nil
	// fmt.Println(filename)
}

func newUpload(filePath string) error {
	filename := filepath.Base(filePath)

	bodyBuf := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuf)
	fileWriter, err := bodyWriter.CreateFormFile("filename", filename)
	if err != nil {
		fmt.Println("fail to create form file")
		return err
	}

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("fail to open file: %s\n", filePath)
		return err
	}
	defer file.Close()

	_, err = io.Copy(fileWriter, file)
	if err != nil {
		fmt.Println("fail to copy content from origin file to form file")
		return err
	}

	contentType := bodyWriter.FormDataContentType()
	bodyWriter.Close()
	response, err := http.Post(tool.Config.MasterURL+"newUpload", contentType, bodyBuf)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer response.Body.Close()

	// 10.10任务
	// 解决master和client的通信问题：使用socket（服务端部署到云服务器时，注意要使用内网ip）
	// 用socket解决这个问题很优雅（用信道channel）。但同时考虑使用http的可行性
	if response.StatusCode != http.StatusOK {
		fmt.Printf("fail to upload file: %s\n", filePath)
		return errors.New("fail to upload file")
	} else {
		msg, _ := ioutil.ReadAll(response.Body)
		if string(msg) == "1" {
			fmt.Println("断点续传")
		} else if string(msg) == "2" {
			fmt.Println("hello")
		}
	}

	return nil
	// fmt.Println(filename)
}

func tcpUpload(filePath string) {
	server := tool.Config.MasterIpPort
	tcpAddr, err := net.ResolveTCPAddr("tcp4", server)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
	fmt.Println("connection success")

	buffer := make([]byte, 2048)

	// 发送文件名
	fileName := filepath.Base(filePath)
	conn.Write([]byte(fileName))
	n, _ := conn.Read(buffer)
	if string(buffer[:n]) != "ok" {
		fmt.Println("[error] 发送文件名失败")
		os.Exit(-1)
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
				fmt.Printf("waiting server back msg error: %v", err)
				os.Exit(-1)
			}
			if string(buffer[:n]) == "end" {
				fmt.Println("文件上传成功")
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
		fmt.Printf("打开文件%s失败", filePath)
		os.Exit(-1)
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

		//conn.Write(buf[:n]) // 我猜这里缓冲区会阻塞，
	}
	// content, err := os.ReadFile(filePath)
	// if err != nil {
	// 	fmt.Println("fail to read from file: " + filePath)
	// 	os.Exit(-1)
	// }
	// conn.Write(content)

	wg.Wait()
	// for {
	// 	_, err = conn.Read(buffer)
	// 	if err != nil {
	// 		fmt.Printf("waiting server back msg error: %v", err)
	// 		os.Exit(-1)
	// 	}
	// 	if string(buffer) == "end" {
	// 		fmt.Println("文件上传成功")
	// 		break
	// 	} else {
	// 		fmt.Println(string(buffer))
	// 		conn.Write([]byte("ok"))
	// 	}
	// }
}

// 用于批量移除指定目录中的指定后缀文件
func removeFilesWithSuffix(prefix []string, suffix, dir string) {
	for _, p := range prefix {
		os.Remove(dir + "/" + p + suffix)
	}
}

// 向指定chunkServer获取指定的chunk
func getChunk(location string, chunk string) ([]byte, error) {
	fmt.Println(location)
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
func download(filename string) {
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

				//response, err := http.PostForm(locationStr+"/download", url.Values{
				//	"chunks": []string{tmpFileChunkStr},
				//})
				//if err != nil {
				//	tip("error", "向chunkServer发送HTTP请求失败")
				//	return
				//}
				//if response.StatusCode != http.StatusOK {
				//	responseBody, _ := io.ReadAll(response.Body)
				//	msg := fmt.Sprintf("http请求错误，状态码：%v，响应：%v", response.StatusCode, responseBody)
				//	tip("error", msg)
				//	return
				//}
				//

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
	var strOfChunksInterface []string // 保存chunsInterface的string类型拷贝
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
	targetFile, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
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

	//var buf bytes.Buffer // 用于拼接所有的chunk
	//
	//rand.Seed(time.Now().Unix())
	//for _, chunkInterface := range chunksInterface { // 依此枚举chunkHandle
	//	chunkStr := StrInterface(chunkInterface)
	//	locationsInterface := res[chunkStr].([]interface{})
	//	locationNum := len(locationsInterface)
	//	// 选择任意一个location
	//	index := rand.Intn(locationNum)
	//	// 向该chunkServer发送获取该chunk的请求，需要指定chunkHandle
	//	locationInterface := locationsInterface[index]
	//	locationStr := StrInterface(locationInterface)
	//
	//	fmt.Println("here!" + locationStr + "/download")
	//	response, err := http.PostForm(locationStr+"/download", url.Values{
	//		"chunk": []string{chunkStr},
	//	})
	//	if err != nil {
	//		fmt.Println("err")
	//		return err
	//	}
	//	if response.StatusCode != http.StatusOK {
	//		msg, _ := io.ReadAll(response.Body)
	//		fmt.Println(string(msg))
	//		// fmt.Println("bad code")
	//		return errors.New(response.Status)
	//	}
	//	content, _ := io.ReadAll(response.Body)
	//	buf.Write(content)
	//}
	//
	//file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
	//if err != nil {
	//	return err
	//}
	//file.Write(buf.Bytes())
	//file.Close()
	//return nil
}

// 向master询问文件列表
func list() error {
	response, err := http.Get(tool.Config.MasterURL + "list")
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		fmt.Println("bad code")
		return errors.New(response.Status)
	}

	msg, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	res := []string{}
	err = json.Unmarshal(msg, &res)
	if err != nil {
		return err
	}
	for i, v := range res {
		fmt.Printf("%d. %s\n", i+1, v)
	}
	return nil
}

func main() {
	flag.Parse()

	switch *action {
	case "upload":
		err := upload(*path)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		fmt.Println("The file was uploaded successfully")
	case "newUpload":
		err := newUpload(*path)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		fmt.Println("The file was uploaded successfully")
	case "tcpUpload":
		tcpUpload(*path)
	case "list":
		// ...
		err := list()
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
	case "download":
		// ...
		download(*path)

	default:
		fmt.Printf("unknown action: %s\n", *action)
		os.Exit(-1)
	}
}
