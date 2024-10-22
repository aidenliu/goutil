package mongodb

import (
	"context"
	"errors"
	"github.com/aidenliu/goutil/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"time"
)

type MConfig struct {
	ConfigKey        string
	Host             string
	ConnectTimeout   time.Duration
	PingTimeInterval time.Duration
}

type Mgo struct {
	*mongo.Client
}

type FindOptions = options.FindOptions
type InsertOneResult = mongo.InsertOneResult
type UpdateResult = mongo.UpdateResult
type DeleteResult = mongo.DeleteResult

func New(mc MConfig) (*Mgo, error) {
	var uri string
	if mc.ConfigKey != "" {
		mConfig := config.Service(mc.ConfigKey)
		uri = mConfig["host"]
	} else {
		uri = mc.Host
	}
	if mc.ConnectTimeout == 0 {
		mc.ConnectTimeout = time.Second * 3
	}
	if mc.PingTimeInterval == 0 {
		mc.PingTimeInterval = time.Second * 3
	}
	mgo := new(Mgo)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri).SetTimeout(mc.ConnectTimeout))
	if err != nil {
		log.Println("mongo connect err:", err)
		return nil, err
	}
	if err := client.Ping(context.TODO(), readpref.Primary()); err != nil {
		return nil, err
	}
	mgo.Client = client
	go func() {
		for {
			if err := mgo.Client.Ping(context.TODO(), readpref.Primary()); err != nil {
				mgo.Client, _ = mongo.Connect(context.TODO(), options.Client().ApplyURI(uri).SetTimeout(mc.ConnectTimeout))
				log.Println("mongoDB collect ping failure", err)
			}
			time.Sleep(mc.PingTimeInterval)
		}
	}()
	return mgo, nil
}

func (mgo *Mgo) IsNotFound(err error) bool {
	return errors.Is(err, mongo.ErrNoDocuments)
}

func (mgo *Mgo) Count(database string, collection string, filter any) int64 {
	count, _ := mgo.Database(database).Collection(collection).CountDocuments(context.TODO(), filter, nil)
	return count
}

func (mgo *Mgo) FindOne(database string, collection string, filter any, result any) error {
	return mgo.Database(database).Collection(collection).FindOne(context.TODO(), filter).Decode(result)
}

func (mgo *Mgo) Find(database string, collection string, filter any, result any, opts *FindOptions) error {
	cursor, err := mgo.Database(database).Collection(collection).Find(context.TODO(), filter, opts)
	if err == nil {
		defer cursor.Close(context.TODO())
		return cursor.All(context.TODO(), result)
	}
	return err
}

func (mgo *Mgo) InsertOne(database string, collection string, document any) (*InsertOneResult, error) {
	return mgo.Database(database).Collection(collection).InsertOne(context.TODO(), document)
}

func (mgo *Mgo) UpdateOne(database string, collection string, filter any, data any) (*UpdateResult, error) {
	return mgo.Database(database).Collection(collection).UpdateOne(context.TODO(), filter, bson.M{"$set": data})
}

func (mgo *Mgo) Upsert(database string, collection string, filter any, data any) (*UpdateResult, error) {
	opts := options.Update().SetUpsert(true)
	return mgo.Database(database).Collection(collection).UpdateOne(context.TODO(), filter, bson.M{"$set": data}, opts)
}

func (mgo *Mgo) UpdateMany(database string, collection string, filter any, data any) (*UpdateResult, error) {
	return mgo.Database(database).Collection(collection).UpdateMany(context.TODO(), filter, bson.M{"$set": data})
}

func (mgo *Mgo) FindOneAndUpdate(database string, collection string, filter any, data any, result any) error {
	r := mgo.Database(database).Collection(collection).FindOneAndUpdate(context.TODO(), filter, bson.M{"$set": data})
	if r.Err() != nil {
		return r.Err()
	}
	return r.Decode(result)
}

func (mgo *Mgo) DeleteOne(database, collection string, filter any) (*DeleteResult, error) {
	return mgo.Database(database).Collection(collection).DeleteOne(context.TODO(), filter)
}

func (mgo *Mgo) DeleteMany(database, collection string, filter any) (*DeleteResult, error) {
	return mgo.Database(database).Collection(collection).DeleteMany(context.TODO(), filter)
}
