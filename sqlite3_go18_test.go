// Copyright (C) 2014 Yasuhiro Matsumoto <mattn.jp@gmail.com>.
//
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.

// +build go1.8

package sqlite3

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

func TestNamedParams(t *testing.T) {
	tempFilename := TempFilename(t)
	defer os.Remove(tempFilename)
	db, err := sql.Open("sqlite3", tempFilename)
	if err != nil {
		t.Fatal("Failed to open database:", err)
	}
	defer db.Close()

	_, err = db.Exec(`
	create table foo (id integer, name text, extra text);
	`)
	if err != nil {
		t.Error("Failed to call db.Query:", err)
	}

	_, err = db.Exec(`insert into foo(id, name, extra) values(:id, :name, :name)`, sql.Named("name", "foo"), sql.Named("id", 1))
	if err != nil {
		t.Error("Failed to call db.Exec:", err)
	}

	row := db.QueryRow(`select id, extra from foo where id = :id and extra = :extra`, sql.Named("id", 1), sql.Named("extra", "foo"))
	if row == nil {
		t.Error("Failed to call db.QueryRow")
	}
	var id int
	var extra string
	err = row.Scan(&id, &extra)
	if err != nil {
		t.Error("Failed to db.Scan:", err)
	}
	if id != 1 || extra != "foo" {
		t.Error("Failed to db.QueryRow: not matched results")
	}
}

var (
	testTableStatements = []string{
		`DROP TABLE IF EXISTS test_table`,
		`
CREATE TABLE IF NOT EXISTS test_table (
	key1      VARCHAR(64) PRIMARY KEY,
	key_id    VARCHAR(64) NOT NULL,
	key2      VARCHAR(64) NOT NULL,
	key3      VARCHAR(64) NOT NULL,
	key4      VARCHAR(64) NOT NULL,
	key5      VARCHAR(64) NOT NULL,
	key6      VARCHAR(64) NOT NULL,
	data      BLOB        NOT NULL
);`,
	}
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	sqliteAddress = "file::memory:?mode=memory&cache=shared"
	rowCount      = 10000
	testCount     = 20000
)

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func initDatabase(t *testing.T, db *sql.DB, rowCount int64) {
	t.Logf("Executing db initializing statements")
	for _, query := range testTableStatements {
		_, err := db.Exec(query)
		if err != nil {
			t.Fatal(err)
		}
	}
	query := `INSERT INTO test_table
		(key1, key_id, key2, key3, key4, key5, key6, data)
		VALUES
		(?, ?, ?, ?, ?, ?, ?, ?);`
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := tx.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	for i := int64(0); i < rowCount; i++ {
		args := []interface{}{
			randStringBytes(50),
			fmt.Sprint(i),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(50),
			randStringBytes(2048),
		}
		_, err := stmt.Exec(args...)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}

func runQueryContext(t *testing.T, db *sql.DB, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	query := "SELECT key1, key_id, key2, key3, key4, key5, key6, data FROM test_table"
	rows, err := db.QueryContext(ctx, query)
	defer rows.Close()
	if err != nil {
		return err
	}
	count := 0
	for rows.Next() {
		var key1, keyid, key2, key3, key4, key5, key6 string
		var data []byte
		err = rows.Scan(&key1, &keyid, &key2, &key3, &key4, &key5, &key6, &data)
		if err != nil {
			return err
		}
		count++
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	if count != rowCount {
		t.Fatal("unexpected row count")
	}
	return nil
}

func getMaxTimeout(t *testing.T, db *sql.DB) int64 {
	nilCount := 0
	errCount := 0
	for i := 1; i <= 60; i++ {
		timeout := time.Duration(int64(1)<<uint(i)) * time.Nanosecond
		err := runQueryContext(t, db, timeout)
		if err == nil {
			nilCount++
		} else {
			if err != context.DeadlineExceeded {
				t.Fatal(err)
			}
			errCount++
		}
		if nilCount > 0 {
			if errCount == 0 {
				t.Fatal("execution time shorter than expected")
			}
			return int64(1) << uint(i)
		}
	}
	t.Fatal("execution time is longer than expected")
	return 0
}

func TestQueryContext(t *testing.T) {
	db, err := sql.Open("sqlite3", sqliteAddress)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	initDatabase(t, db, rowCount)
	maxTimeout := getMaxTimeout(t, db)
	for i := 0; i < testCount; i++ {
		timeout := time.Duration(rand.Int63n(maxTimeout)) * time.Nanosecond
		err := runQueryContext(t, db, timeout)
		if (err != nil) && (err != context.DeadlineExceeded) {
			t.Fatal(err)
		}
	}
}
