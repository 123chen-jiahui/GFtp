package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
)

var port = flag.String("port", "", "port of chunkServer")

func getLocation() string {
	netInterfaces, err := net.Interfaces()
	if err != nil {
		fmt.Println("net.Interfaces failed, err:", err.Error())
	}

	for i := 0; i < len(netInterfaces); i++ {
		if (netInterfaces[i].Flags & net.FlagUp) != 0 {
			addrs, _ := netInterfaces[i].Addrs()

			for _, address := range addrs {
				if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
					if ipnet.IP.To4() != nil {
						return ipnet.IP.String()
						// fmt.Println(ipnet.IP.String())
					}
				}
			}
		}
	}
	return ""
}

func greetMaster() error {
	files, err := ioutil.ReadDir("./files")
	if err != nil {
		return err
	}
	chunks := []string{}
	for _, file := range files {
		chunkHandle := strings.Split(file.Name(), ".")[0]
		chunks = append(chunks, chunkHandle)
	}

	location := getLocation()
	if location == "" {
		return errors.New("fail to get ip address")
	}
	// location = "http://" + location + tool.Config.Port
	location = "http://" + location + *port

	// url不应该直接写出来
	response, err := http.PostForm("http://localhost:8080/report", url.Values{
		"chunks":   chunks,
		"location": []string{location},
	})
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return errors.New("fail to contact with master")
	}
	return nil
}

// 处理master发过来的chunk
func handleChunk(w http.ResponseWriter, r *http.Request) {
	fmt.Println(r.Method)
	if r.Method == "POST" { // 处理master发来的POST请求（添加chunk）
		file, handle, err := r.FormFile("filename")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer file.Close()

		f, err := os.OpenFile(path.Join("./files", handle.Filename), os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			w.WriteHeader((http.StatusBadRequest))
			return
		}
		defer f.Close()

		io.Copy(f, file)
	} else if r.Method == "GET" { // 处理master发来的GET请求（返回对应的chunk内容）
		query := r.URL.Query()
		chunk := query["chunkHandle"][0]
		chunkPath := "./files/" + string(chunk) + ".chunk"
		msg, err := os.ReadFile(chunkPath)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "can not open file: %s.chunk", chunk)
			return
		}
		w.Write(msg)
	}
}

// 处理client发送过来的下载chunk的请求
func download(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	chunkHandle := r.Form["chunk"][0]
	filename := "./files/" + chunkHandle + ".chunk"
	fmt.Println(filename)

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "can not open file: %s", filename)
		return
	}
	// w.Header().Set("Content-Type", "application/octet-stream")
	_, err = w.Write(content)
	if err == nil {
		fmt.Println("no problem")
	}
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Println("aaaaaa")
		fmt.Println(err)
	}
}

func main() {
	flag.Parse()
	*port = ":" + *port
	fmt.Println(*port)

	// var port string
	// fmt.Print("input port: ")
	// fmt.Scanf("%s", &port)
	// fmt.Println(port)
	// tool.Config.Port = port

	// 联系master，并表明自己的location以及chunks
	err := greetMaster()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	http.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		// do nothing
	})

	http.HandleFunc("/chunk", handleChunk)

	http.HandleFunc("/download", download)

	http.ListenAndServe(*port, nil)
}
