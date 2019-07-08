package sqlt

import (
	"errors"
	"strings"
)

// Connect to a database and verify with a ping.
func Connect(driverName, dataSourceName string, groupNames ...string) (*DB, error) {
	var groupName string
	if len(groupNames) > 0 {
		groupName = groupNames[0]
	}
	db, err := OpenWithName(driverName, dataSourceName, groupName)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// MustConnect connects to a database and panics on error.
func MustConnect(driverName, dataSourceName string, groupNames ...string) *DB {
	db, err := Connect(driverName, dataSourceName, groupNames...)
	if err != nil {
		panic(err)
	}
	return db
}

// Close connection
func (db *DB) Close() (err error) {
	var errs []string
	for i := range db.sqlxdb {
		if err := db.sqlxdb[i].Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	db.StopBeat()
	if len(errs) > 0 {
		err = errors.New(strings.Join(errs, "\n"))
	}
	return
}

// Unsafe returns a version of DB which will silently succeed to scan when
// columns in the SQL result have no fields in the destination struct.
// sqlx.Stmt and sqlx.Tx which are created from this DB will inherit its
// safety behavior.
func (db *DB) Unsafe() *DB {
	return &DB{
		sqlxdb:     db.sqlxdb,
		activedb:   db.activedb,
		inactivedb: db.inactivedb,
		dsn:        db.dsn,
		driverName: db.driverName,
		groupName:  db.groupName,
		length:     db.length,
		count:      db.count,
		stats:      db.stats,
		heartBeat:  db.heartBeat,
		stopBeat:   db.stopBeat,
		lastBeat:   db.lastBeat,
		debug:      db.debug,
		unsafe:     true,
	}
}
