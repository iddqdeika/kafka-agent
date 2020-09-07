package nethelper

import (
	"fmt"
	"net"
)

func GetCurrentAddr(port int) (string, error) {
	ip, err := GetCurrentIp()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%v:%d", ip, port), nil
}

func GetCurrentIp() (string, error) {
	addrs, err := net.InterfaceAddrs()

	if err != nil {
		fmt.Println(err)
	}
	for _, address := range addrs {
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("cant get address")
}
