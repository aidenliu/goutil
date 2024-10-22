package config

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/aidenliu/goutil"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
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

func init() {
	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ldate)
	_ = InitLoad()
}

func getConfigPath() string {
	file, _ := exec.LookPath(os.Args[0])
	configPath, _ := filepath.Abs(file)
	configPath = filepath.Dir(configPath) + "/conf"
	if !goutil.FileExists(configPath) {
		currentPath, _ := os.Getwd()
		configPath = currentPath + "/bin/conf"
	}
	if runEnv := os.Getenv("RUN_ENV"); runEnv != "" {
		configPath += "/" + runEnv
	} else {
		configPath += "/rc"
	}
	return configPath
}

func ParseFile(fileName string, data any) error {
	configPath := getConfigPath()
	fileFullName := fmt.Sprintf("%s/%s", configPath, fileName)
	if goutil.FileExists(fileFullName) {
		vp := viper.New()
		vp.SetConfigFile(fileFullName)
		if err := vp.ReadInConfig(); err != nil {
			return err
		} else {
			if err := vp.Unmarshal(data); err != nil {
				return err
			}
			// 自动载入配置
			vp.OnConfigChange(func(e fsnotify.Event) {
				err := vp.Unmarshal(data)
				log.Printf("config file[%s] has changed, reload[%s]\n", e.Name, err)
			})
			vp.WatchConfig()
			return nil
		}
	} else {
		return fmt.Errorf("not find the config file:%s", fileFullName)
	}
}

func InitLoad() error {
	configPath := getConfigPath()
	commonPath := ""
	if index := strings.LastIndex(configPath, "/"); index != -1 {
		commonPath = configPath[:index] + "/common"
	}
	// 通用常量
	if goutil.FileExists(commonPath + "/constant.yaml") {
		vp := viper.New()
		vp.SetConfigFile(commonPath + "/constant.yaml")
		if err := vp.ReadInConfig(); err != nil {
			return err
		} else {
			if err := vp.Unmarshal(&CommonConstant); err != nil {
				return err
			}
			// 自动载入配置
			vp.OnConfigChange(func(e fsnotify.Event) {
				err := vp.Unmarshal(&CommonConstant)
				log.Printf("config file[%s] has changed, reload[%s]\n", e.Name, err)
			})
			vp.WatchConfig()
		}
	}
	// 环境常量
	if goutil.FileExists(configPath + "/envconstant.yaml") {
		vp := viper.New()
		vp.SetConfigFile(configPath + "/envconstant.yaml")
		if err := vp.ReadInConfig(); err != nil {
			return err
		} else {
			if err := vp.Unmarshal(&EnvConstant); err != nil {
				return err
			}
			// 自动载入配置
			vp.OnConfigChange(func(e fsnotify.Event) {
				err := vp.Unmarshal(&EnvConstant)
				log.Printf("config file[%s] has changed, reload[%s]\n", e.Name, err)
			})
			vp.WatchConfig()
		}
	}
	// 服务
	if goutil.FileExists(configPath + "/service.yaml") {
		vp := viper.New()
		vp.SetConfigFile(configPath + "/service.yaml")
		if err := vp.ReadInConfig(); err != nil {
			return err
		} else {
			if err := vp.Unmarshal(&ServiceConfig); err != nil {
				return err
			}
			// 自动载入配置
			vp.OnConfigChange(func(e fsnotify.Event) {
				err := vp.Unmarshal(&ServiceConfig)
				log.Printf("config file[%s] has changed, reload[%s]\n", e.Name, err)
			})
			vp.WatchConfig()
		}
	}
	// 第三方服务配置
	if goutil.FileExists(configPath + "/vendor.yaml") {
		vp := viper.New()
		vp.SetConfigFile(configPath + "/vendor.yaml")
		if err := vp.ReadInConfig(); err != nil {
			return err
		} else {
			if err := vp.Unmarshal(&VendorConfig); err != nil {
				return err
			}
			// 自动载入配置
			vp.OnConfigChange(func(e fsnotify.Event) {
				err := vp.Unmarshal(&VendorConfig)
				log.Printf("config file[%s] has changed, reload[%s]\n", e.Name, err)
			})
			vp.WatchConfig()
		}
	}
	// 数据库服务配置
	if goutil.FileExists(configPath + "/db.yaml") {
		vp := viper.New()
		vp.SetConfigFile(configPath + "/db.yaml")
		if err := vp.ReadInConfig(); err != nil {
			return err
		} else {
			if err := vp.Unmarshal(&DbConfig); err != nil {
				return err
			}
			// 自动载入配置
			vp.OnConfigChange(func(e fsnotify.Event) {
				err := vp.Unmarshal(&DbConfig)
				log.Printf("config file[%s] has changed, reload[%s]\n", e.Name, err)
			})
			vp.WatchConfig()
		}
	}
	return nil
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
