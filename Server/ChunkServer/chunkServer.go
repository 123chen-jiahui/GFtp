package main

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/tool"
)

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
	location = "http://" + location + tool.Config.Port

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
func addChunk(w http.ResponseWriter, r *http.Request) {
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
}

func main() {

	// 联系master，并表明自己的location以及chunks
	err := greetMaster()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	http.HandleFunc("/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		// do nothing
	})

	http.HandleFunc("/chunk", addChunk)

	http.ListenAndServe(tool.Config.Port, nil)
}
