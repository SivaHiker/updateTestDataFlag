package main

import (
	"gopkg.in/mgo.v2"
	"fmt"
	"gopkg.in/mgo.v2/bson"
)

var jobs chan UserInfo
var done chan bool
var counter int64
var c *mgo.Collection

func main() {


	jobs = make(chan UserInfo, 10000)
	done = make(chan bool, 1)

	session, err := mgo.Dial("10.15.0.145")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	c = session.DB("userlist").C("newuserdata")
	fmt.Println(c.Name)

	for w := 1; w <= 500; w++ {
		go workerPool()
	}

	item := UserInfo{}

	find := c.Find(bson.M{"flag":true})

	items := find.Iter()

	for items.Next(&item) {
		fmt.Println("Entered inside")
		jobs<- item
	}

	<-done
	fmt.Println("Total Updated Documents ",counter)
}

func workerPool() {
	for (true) {
		select {
		case item,ok := <-jobs:
			if ok {
				fmt.Println(item.ID)
				err := c.Update(item, bson.M{"$set": bson.M{"flag": false}})
				fmt.Println(err)
				counter++
				fmt.Println("Migrated records till now --- >", counter)
			}
		case <-done:
			done<-true
		}
	}
}

type UserInfo struct {
	ID       bson.ObjectId `bson:"_id,omitempty"`
	UserData UserData `json:"UserData"`
	Flag   bool `json:"flag"`
	Active bool `json:"active"`
}

//type UserData struct {
//	Msisdn string `json:"msisdn"`
//	Token  string `json:"token"`
//	UID    string `json:"uid"`
//}

type UserData struct {
	Msisdn        string `json:"msisdn"`
	Token         string `json:"token"`
	UID           string `json:"uid"`
	Platformuid   string `json:"platformuid"`
	Platformtoken string `json:"platformtoken"`
}
