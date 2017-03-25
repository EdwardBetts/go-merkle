package cmd

import (
	"fmt"
	"path"

	"github.com/spf13/cobra"

	cmn "github.com/tendermint/go-common"
	db "github.com/tendermint/go-db"
	merkle "github.com/tendermint/go-merkle"
)

var (
	dbName    string
	dbDir     string
	cacheSize int
)

var dumpCmd = &cobra.Command{
	Run:   DumpDatabase,
	Use:   "dump",
	Short: "Dump a database",
	Long:  `Dump all of the data for an underlying persistent database`,
}

func init() {
	RootCmd.AddCommand(dumpCmd)
	dumpCmd.Flags().StringVarP(&dbName, "name", "n", "merkleeyes", "Name db dir")
	dumpCmd.Flags().StringVarP(&dbDir, "path", "p", "./", "Dir path to DB")
	dumpCmd.Flags().IntVarP(&cacheSize, "cachesize", "c", 10000, "Size of the Cache")
}

func DumpDatabase(cmd *cobra.Command, args []string) {
	dbPath := path.Join(dbDir, dbName+".db")

	if !cmn.FileExists(dbPath) {
		cmn.Exit("No existing database: " + dbPath)
	}

	fmt.Printf("Dumping DB %s (%s)...\n", dbName, dbType)

	database := db.NewDB(dbName, db.LevelDBBackendStr, "./")

	fmt.Printf("Database: %v\n", database)

	tree := merkle.NewIAVLTree(cacheSize, database)

	fmt.Printf("Tree: %v\n", tree)

	tree.Dump(nil)
}
