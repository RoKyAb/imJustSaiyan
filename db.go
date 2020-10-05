package main

import (
	"database/sql"
	"flag"
	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"os"
	"strings"
	"time"
)

var gokuDB = flag.String(
	"goku_db",
	"mysql://root@unix("+getSocketFile()+")/goku?",
	"ledger DB URI")

var gokuDBRO = flag.String(
	"ledger_db_readonly",
	"mysql://root@unix("+getSocketFile()+")/goku?",
	"ledger DB RO URI")

var dbMaxOpenConns = flag.Int("db_max_open_conns", 100, "Maximum number of open database connections")
var dbMaxIdleConns = flag.Int("db_max_idle_conns", 50, "Maximum number of idle database connections")
var dbConnMaxLifetime = flag.Duration("db_conn_max_lifetime", time.Minute, "Maximum time a single database connection can be left open")


func getSocketFile() string {
	var sock = "/tmp/mysql.sock"
	if _, err := os.Stat(sock); os.IsNotExist(err) {
		// try common linux/Ubuntu socket file location
		return "/var/run/mysqld/mysqld.sock"
	}
	return sock
}

func connectToGokuDB() (*GokuDB, error) {
	dbc, err := connect("mysql", *gokuDB)
	if err != nil {
		return nil, err
	}

	var db GokuDB
	db.DB = dbc

	replica, err := connect("mysql", *gokuDBRO)
	if err != nil {
		return nil, err
	}
	db.ReplicaDB = replica

	return &db, nil
}

type GokuDB struct {
	DB        *sql.DB
	ReplicaDB *sql.DB
}

func connect(driver, connectStr string) (*sql.DB, error) {
	connectStr, err := build(connectStr)
	if err != nil {
		return nil, err
	}

	dbc, err := sql.Open(driver, connectStr)
	if err != nil {
		return nil, err
	}

	dbc.SetMaxOpenConns(*dbMaxOpenConns)
	dbc.SetMaxIdleConns(*dbMaxIdleConns)
	dbc.SetConnMaxLifetime(*dbConnMaxLifetime)

	return dbc, nil
}

func build(connectStr string) (string, error) {
	const prefix = "mysql://"
	if !strings.HasPrefix(connectStr, prefix) {
		return "", errors.New("db: URI is missing mysql:// prefix")
	}
	connectStr = connectStr[len(prefix):]

	dsn, err := mysql.ParseDSN(connectStr)
	if err != nil {
		return "", err
	}

	if dsn.Params == nil {
		dsn.Params = make(map[string]string)
	}
	dsn.Params["parseTime"] = "true"               // time.Time for datetime
	dsn.Params["collation"] = "utf8mb4_general_ci" // non-BMP unicode chars

	return dsn.FormatDSN(), nil
}

func (db *GokuDB) CloseAll() {
	if db.DB != nil {
		if err := db.DB.Close(); err != nil {
			// NoReturnErr: Probably shutting down.
			log.Error(nil, errors.Wrap(err, "error closing master db connection"))
		}
	}
	if db.ReplicaDB != nil {
		if err := db.ReplicaDB.Close(); err != nil {
			// NoReturnErr: Probably shutting down.
			log.Error(nil, errors.Wrap(err, "error closing replica db connection"))
		}
	}
}