package db

import (
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"sync"
)

var connects = make(map[string]*gorm.DB)
var lock sync.Mutex

func New(dsn string) (*gorm.DB, error) {
	lock.Lock()
	defer lock.Unlock()
	db, ok := connects[dsn]
	if !ok {
		dbNew, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Error),
		})
		if err != nil {
			return nil, err
		}
		mysqlDB, _ := dbNew.DB()
		mysqlDB.SetMaxOpenConns(3)
		connects[dsn] = dbNew
		db = dbNew
	}
	return db, nil
}
