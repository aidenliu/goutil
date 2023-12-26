package db

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"log"
	"os"
	"sync"
	"time"
)

var connects = make(map[string]*gorm.DB)
var lock sync.Mutex

func New(dsn string) (*gorm.DB, error) {
	lock.Lock()
	defer lock.Unlock()
	if db, ok := connects[dsn]; ok {
		mysqlDB, _ := db.DB()
		if mysqlDB.Ping() == nil {
			return db, nil
		}
	}
	newLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             3 * time.Second,
			LogLevel:                  logger.Error,
			IgnoreRecordNotFoundError: true,
		},
	)
	dbNew, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger:               newLogger,
		DisableAutomaticPing: true,
	})
	if err != nil {
		return nil, err
	}
	mysqlDB, _ := dbNew.DB()
	mysqlDB.SetMaxIdleConns(3)
	mysqlDB.SetMaxOpenConns(100)
	connects[dsn] = dbNew
	return dbNew, nil
}
