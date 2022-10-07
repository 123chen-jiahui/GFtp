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
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/tool"
)

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
	case "list":
		// ...

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

	http.ListenAndServe(tool.Config.Port, nil)
}
