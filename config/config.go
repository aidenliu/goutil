package config

import (
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

// CommonConstant 通用常量配置
var CommonConstant = make(map[string]map[string]string)

// EnvConstant 环境相关常量配置
var EnvConstant = make(map[string]map[string]string)

// ServiceConfig 服务配置
var ServiceConfig = make(map[string]map[string]string)

// VendorConfig 第三方服务配置
var VendorConfig = make(map[string]map[string]string)

func init() {
	file, _ := exec.LookPath(os.Args[0])
	configPath, _ := filepath.Abs(file)
	configPath = filepath.Dir(configPath) + "/conf"

	commonPath := configPath + "/common/"
	if _, err := os.Stat("/www/web/Product"); err == nil {
		configPath += "/product"
	} else {
		configPath += "/dev"
	}
	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ldate)
	viper.AddConfigPath(commonPath)
	viper.AddConfigPath(configPath)
	viper.SetConfigType("yaml")
	// 通用常量
	viper.SetConfigName("constant.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Panicln(err)
	} else {
		if err := viper.Unmarshal(&CommonConstant); err != nil {
			log.Panicln(err)
		}
	}
	// 环境常量
	viper.SetConfigName("envconstant.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Panicln(err)
	} else {
		if err := viper.Unmarshal(&EnvConstant); err != nil {
			log.Panicln(err)
		}
	}
	// 服务
	viper.SetConfigName("service.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Panicln(err)
	} else {
		if err := viper.Unmarshal(&ServiceConfig); err != nil {
			log.Panicln(err)
		}
	}
	// 第三方服务配置
	viper.SetConfigName("vendor.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Panicln(err)
	} else {
		if err := viper.Unmarshal(&VendorConfig); err != nil {
			log.Panicln(err)
		}
	}
	// 自动载入配置
	viper.OnConfigChange(func(e fsnotify.Event) {
		log.Println(e.Name)
	})
	viper.WatchConfig()
}

func ConstantGroup(constantType, groupKey string) map[string]string {
	var constConfig map[string]map[string]string
	switch constantType {
	case "common":
		constConfig = CommonConstant
	case "env":
		constConfig = EnvConstant
	}
	if group, ok := constConfig[groupKey]; ok {
		return group
	}
	return nil
}

// Constant 获取常量配置项
func Constant(constantType, groupKey, key string) string {
	group := ConstantGroup(constantType, groupKey)
	if value, ok := group[key]; ok {
		return value
	}
	return ""
}

// Service 服务
func Service(key string) map[string]string {
	c, exists := ServiceConfig[key]
	if !exists {
		return nil
	}
	return c
}

// Vendor 第三方服务配置
func Vendor(key string) map[string]string {
	c, exists := VendorConfig[key]
	if !exists {
		return nil
	}
	return c
}
