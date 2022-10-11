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

func download(filename string) error {
	response, err := http.PostForm(tool.Config.MasterURL+"download", url.Values{
		"filename": []string{filename},
		"location": []string{tool.Location},
	})
	// 不知道没有成功发送请求是不是错误...
	// 答：不是错误
	if err != nil {
		fmt.Println("err")
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

	// res是string到interface的映射
	res := make(map[string]interface{})
	err = json.Unmarshal(msg, &res)
	if err != nil {
		return err
	}

	// =====test
	// fmt.Printf("test: len %d\n", len(res["1"].([]interface{})))
	// =====

	// 接下来向各个chunkServer发送请求
	chunksInterface := res["chunks"].([]interface{})
	// chunkNum := len(chunksInterface) // 要下载的文件的chunk数
	var buf bytes.Buffer // 用于拼接所有的chunk

	rand.Seed(time.Now().Unix())
	for _, chunkInterface := range chunksInterface { // 依此枚举chunkHandle
		chunkStr := StrInterface(chunkInterface)
		locationsInterface := res[chunkStr].([]interface{})
		locationNum := len(locationsInterface)
		// 选择任意一个location
		index := rand.Intn(locationNum)
		// 向该chunkServer发送获取该chunk的请求，需要指定chunkHandle
		locationInterface := locationsInterface[index]
		locationStr := StrInterface(locationInterface)

		fmt.Println("here!" + locationStr + "/download")
		response, err := http.PostForm(locationStr+"/download", url.Values{
			"chunk": []string{chunkStr},
		})
		if err != nil {
			fmt.Println("err")
			return err
		}
		if response.StatusCode != http.StatusOK {
			msg, _ := io.ReadAll(response.Body)
			fmt.Println(string(msg))
			// fmt.Println("bad code")
			return errors.New(response.Status)
		}
		content, _ := ioutil.ReadAll(response.Body)
		buf.Write(content)
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	file.Write(buf.Bytes())
	file.Close()
	// fmt.Println(res)
	// tmp := res["chunks"].([]interface{})
	// for _, v := range tmp {
	// 	s := fmt.Sprintf("%v", v)
	// 	fmt.Println(res[StrInterface(v)])
	// 	fmt.Println(s)
	// }
	// fmt.Println(len(res["chunks"].(string)))
	return nil
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
		err := download(*path)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
		fmt.Println("here!")

	default:
		fmt.Printf("unknown action: %s\n", *action)
		os.Exit(-1)
	}
}
