package helpers

import "strconv"

func JoinAddressAndPort(address string, port int) string {
	return address + ":" + strconv.Itoa(port)
}
