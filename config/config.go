package config

import (
	"github.com/aidenliu/goutil"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

type dbConfig struct {
	DbUser    string
	DbPWD     string
	DbName    string
	DbCharset string
	DbSource  []struct {
		TableNum      int
		TableName     []string
		TableIndexLen int
		DbName        string
		DbHost        []string
	}
}

// RunEnv 运行环境
var RunEnv string

// CommonConstant 通用常量配置
var CommonConstant = make(map[string]map[string]string)

// EnvConstant 环境相关常量配置
var EnvConstant = make(map[string]map[string]string)

// ServiceConfig 服务配置
var ServiceConfig = make(map[string]map[string]string)

// VendorConfig 第三方服务配置
var VendorConfig = make(map[string]map[string]string)

// DbConfig 数据库配置
var DbConfig = dbConfig{}

func InitLoad() {
	file, _ := exec.LookPath(os.Args[0])
	configPath, _ := filepath.Abs(file)
	configPath = filepath.Dir(configPath) + "/conf"
	if !goutil.FileExists(configPath) {
		currentPath, _ := os.Getwd()
		configPath = currentPath + "/conf"
	}
	commonPath := configPath + "/common"
	if runEnv := os.Getenv("RUN_ENV"); runEnv != "" {
		configPath += "/" + runEnv
	} else {
		configPath += "/dev"
	}
	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ldate)
	viper.AddConfigPath(commonPath)
	viper.AddConfigPath(configPath)
	viper.SetConfigType("yaml")
	// 通用常量
	if goutil.FileExists(commonPath + "/constant.yaml") {
		viper.SetConfigName("constant.yaml")
		if err := viper.ReadInConfig(); err != nil {
			log.Panicln(err)
		} else {
			if err := viper.Unmarshal(&CommonConstant); err != nil {
				log.Panicln(err)
			}
		}
	}
	// 环境常量
	if goutil.FileExists(configPath + "/envconstant.yaml") {
		viper.SetConfigName("envconstant.yaml")
		if err := viper.ReadInConfig(); err != nil {
			log.Panicln(err)
		} else {
			if err := viper.Unmarshal(&EnvConstant); err != nil {
				log.Panicln(err)
			}
		}
	}
	// 服务
	if goutil.FileExists(configPath + "/service.yaml") {
		viper.SetConfigName("service.yaml")
		if err := viper.ReadInConfig(); err != nil {
			log.Panicln(err)
		} else {
			if err := viper.Unmarshal(&ServiceConfig); err != nil {
				log.Panicln(err)
			}
		}
	}
	// 第三方服务配置
	if goutil.FileExists(configPath + "/vendor.yaml") {
		viper.SetConfigName("vendor.yaml")
		if err := viper.ReadInConfig(); err != nil {
			log.Panicln(err)
		} else {
			if err := viper.Unmarshal(&VendorConfig); err != nil {
				log.Panicln(err)
			}
		}
	}
	// 数据库服务配置
	if goutil.FileExists(configPath + "/db.yaml") {
		viper.SetConfigName("db.yaml")
		if err := viper.ReadInConfig(); err != nil {
			log.Panicln(err)
		} else {
			if err := viper.Unmarshal(&DbConfig); err != nil {
				log.Panicln(err)
			}
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

// Db 数据库配置
func Db() dbConfig {
	return DbConfig
}
