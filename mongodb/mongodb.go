package mongodb

import (
	"context"
	"github.com/aidenliu/goutil/config"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"log"
	"time"
)

type Mgo struct {
	*mongo.Client
}

func New(configKey string) *Mgo {
	mConfig := config.Service(configKey)
	uri := mConfig["host"]
	mgo := new(Mgo)
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri).SetTimeout(3*time.Second))
	if err != nil {
		log.Println("mongo connect err:", err)
	}
	mgo.Client = client
	go func() {
		for {
			err := client.Ping(context.TODO(), readpref.Primary())
			if err != nil {
				mgo.Client, _ = mongo.Connect(context.TODO(), options.Client().ApplyURI(uri).SetTimeout(3*time.Second))
				log.Println("mongoDB collect ping failure", err)
			}
			time.Sleep(3 * time.Second)
		}
	}()
	return mgo
}

func (mgo *Mgo) IsNotFound(err error) bool {
	return err == mongo.ErrNoDocuments
}

func (mgo *Mgo) Count(database string, collection string, filter any) int64 {
	count, _ := mgo.Database(database).Collection(collection).CountDocuments(context.TODO(), filter, nil)
	return count
}

func (mgo *Mgo) FindOne(database string, collection string, filter any, result any) error {
	return mgo.Database(database).Collection(collection).FindOne(context.TODO(), filter).Decode(result)
}

func (mgo *Mgo) Find(database string, collection string, filter any, result any, opts *options.FindOptions) error {
	cursor, err := mgo.Database(database).Collection(collection).Find(context.TODO(), filter, opts)
	if err == nil {
		defer cursor.Close(context.TODO())
		return cursor.All(context.TODO(), result)
	}
	return err
}

func (mgo *Mgo) InsertOne(database string, collection string, document any) (*mongo.InsertOneResult, error) {
	return mgo.Database(database).Collection(collection).InsertOne(context.TODO(), document)
}

func (mgo *Mgo) UpdateOne(database string, collection string, filter any, data any) (*mongo.UpdateResult, error) {
	return mgo.Database(database).Collection(collection).UpdateOne(context.TODO(), filter, bson.M{"$set": data})
}

func (mgo *Mgo) Upsert(database string, collection string, filter any, data any) (*mongo.UpdateResult, error) {
	opts := options.Update().SetUpsert(true)
	return mgo.Database(database).Collection(collection).UpdateOne(context.TODO(), filter, bson.M{"$set": data}, opts)
}

func (mgo *Mgo) UpdateMany(database string, collection string, filter any, data any) (*mongo.UpdateResult, error) {
	return mgo.Database(database).Collection(collection).UpdateMany(context.TODO(), filter, bson.M{"$set": data})
}

func (mgo *Mgo) FindOneAndUpdate(database string, collection string, filter any, data any, result any) error {
	r := mgo.Database(database).Collection(collection).FindOneAndUpdate(context.TODO(), filter, bson.M{"$set": data})
	if r.Err() != nil {
		return r.Err()
	}
	return r.Decode(result)
}

func (mgo *Mgo) DeleteOne(database, collection string, filter any) (*mongo.DeleteResult, error) {
	return mgo.Database(database).Collection(collection).DeleteOne(context.TODO(), filter)
}

func (mgo *Mgo) DeleteMany(database, collection string, filter any) (*mongo.DeleteResult, error) {
	return mgo.Database(database).Collection(collection).DeleteMany(context.TODO(), filter)
}
