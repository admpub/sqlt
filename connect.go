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
