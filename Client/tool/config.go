package tool

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
)

type CONFIG struct {
	MasterURL    string `json:"masterURL"`
	MasterIpPort string `json:"masterIpPort"`
	Port         string `json:"port"`
}

var Config CONFIG
var Location string

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

func init() {
	file, err := os.Open("./setting/setting.json")
	if err != nil {
		fmt.Println("fail to open file: ./setting/setting.json")
		panic(err)
	}

	decoder := json.NewDecoder(file)
	err = decoder.Decode(&Config)
	if err != nil {
		fmt.Println("fail to decode")
		panic(err)
	}
	ipAddress := getLocation()
	if ipAddress == "" {
		fmt.Println("fail to get ip address")
		panic(err)
	}
	Location = "http://" + ipAddress + Config.Port
	fmt.Println(Config)
	fmt.Println(Location)
}
