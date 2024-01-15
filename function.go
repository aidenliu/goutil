package goutil

import (
	"encoding/binary"
	"errors"
	"github.com/neverlee/goyar"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"
)

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~int32 | ~int64 | ~uintptr
}
type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}
type Float interface {
	~float32 | ~float64
}

// Substr 截取子字符串
func Substr(str string, offset ...int) string {
	if len(str) == 0 {
		return str
	}
	strR := []rune(str)
	argLen := len(offset)
	if argLen == 0 {
		return str
	}
	var subStrR []rune
	if argLen == 1 {
		subStrR = strR[offset[0]:]
	} else {
		subStrR = strR[offset[0]:offset[1]]
	}
	return string(subStrR)
}

// StrPad 补全字符串
func StrPad(str string, padLen int, padStr, padType string) string {
	strLen := len(str)
	lenDiff := padLen - strLen
	if lenDiff <= 0 {
		return str
	}
	if padType == "left" {
		str = strings.Repeat(padStr, lenDiff) + str
	} else {
		str += strings.Repeat(padStr, lenDiff)
	}
	return str
}

// InSlice 值是否在Slice里
func InSlice[T Unsigned | Signed | Float | string](value T, s []T) bool {
	for _, v := range s {
		if v == value {
			return true
		}
	}
	return false
}

// RandInt 生成随机整数
func RandInt(n int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(n)
}

// YarRpcCall php YarRPC Server 调用
func YarRpcCall(url, method string, ret interface{}, params ...interface{}) error {
	client := goyar.NewYHClient(url, nil)
	return client.MCall(method, &ret, params...)
}

// IP2long converts a string containing an (IPv4) Internet Protocol dotted address into a long integer
func IP2long(ipAddr string) (uint32, error) {
	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return 0, errors.New("wrong ipAddr format")
	}
	ip = ip.To4()
	return binary.BigEndian.Uint32(ip), nil
}

// Long2ip converts an long integer address into a string in (IPv4) Internet standard dotted format
func Long2ip(ipLong uint32) string {
	ipByte := make([]byte, 4)
	binary.BigEndian.PutUint32(ipByte, ipLong)
	ip := net.IP(ipByte)
	return ip.String()
}

func GetLocalIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, address := range addrs {
		// 检查ip地址判断是否回环地址
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", nil
}

func FileExists(filePath string) bool {
	if _, err := os.Stat(filePath); err != nil && os.IsNotExist(err) {
		return false
	} else {
		return true
	}
}
