package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
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

func main() {

	// 联系master
	err := greetMaster()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	http.ListenAndServe(":8081", nil)
}
