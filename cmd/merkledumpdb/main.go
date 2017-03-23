package main

import (
	"fmt"
	"path"

	kingpin "gopkg.in/alecthomas/kingpin.v2"

	cmn "github.com/tendermint/go-common"
	db "github.com/tendermint/go-db"
	merkle "github.com/tendermint/go-merkle"
)

var (
	dbType    = kingpin.Flag("dbtype", "type of backing db").Short('t').Default("goleveldb").String()
	dbName    = kingpin.Flag("name", "name db dir (without .db)").Short('n').Default("merkleeyes").String()
	dir       = kingpin.Flag("dir", "dir path to db").Short('d').Default("./").String()
	cacheSize = kingpin.Flag("cachesize", "size of the cache").Short('c').Default("10000").Int()
)

func main() {
	kingpin.Parse()

	if !cmn.FileExists(path.Join(*dir, *dbName+".db")) {
		cmn.Exit("No such Database")
	}

	fmt.Printf("Dumping DB %s (%s)...\n", *dbName, *dbType)

	database := db.NewDB(*dbName, db.LevelDBBackendStr, "./")

	fmt.Printf("Database: %v\n", database)

	tree := merkle.NewIAVLTree(*cacheSize, database)

	fmt.Printf("Tree: %v\n", tree)

	tree.Dump(nil)
}
