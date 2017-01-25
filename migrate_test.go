package sqlmigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB

func init() {
	var config = struct {
		dbName, dbPassword, dbUser, dbAddress string
		dbPort                                int
	}{
		dbAddress:  "127.0.0.1",
		dbName:     "sql_migrate",
		dbPassword: "",
		dbUser:     "root",
		dbPort:     3388,
	}

	connection, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True", config.dbUser, config.dbPassword, config.dbAddress, config.dbPort, config.dbName))
	if err != nil {
		panic(err)
	}
	if err := connection.Ping(); err != nil {
		panic(err)
	}
	db = connection
}

func setUp() {
	createTableIfNotExists(db)
	var query = fmt.Sprintf("DELETE FROM %v WHERE LENGTH(`id`) > 0", tableName)
	_, err := db.Exec(query)
	if err != nil {
		panic(err)
	}
}

func TestGetAppliedMigrations(t *testing.T) {
	setUp()

	//create 3 applied migration
	var query = fmt.Sprintf("INSERT INTO %v(`id`,`applied_at`) VALUES(?,?)", tableName)
	db.Exec(query, "1", time.Now())
	db.Exec(query, "2", time.Now())
	db.Exec(query, "3", time.Now())

	applied, err := getAppliedMigrations(db)
	if err != nil {
		t.Fatal(err)
	}
	if len(applied) != 3 {
		t.Errorf("unexpected length of applied migrations. actual %v, expected %v", len(applied), 3)
	}
}

func TestExecMigrations(t *testing.T) {
	clean := func() {
		db.Exec("DROP TABLE IF EXISTS `user_test`")
		db.Exec("DROP TABLE IF EXISTS `user_test2`")
	}
	defer clean()

	var cases = []struct {
		migrations []*Migration
		expected   int
		err        error
	}{
		{[]*Migration{
			&Migration{ID: "1", Up: []string{"CREATE TABLE `user_test` (`id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"}},
		}, 1, nil}, //execute 1 correct query
		{[]*Migration{
			&Migration{ID: "1", Up: []string{"CREATE TABLE `user_test` (`id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"}},
			&Migration{ID: "2", Up: []string{"CREATE TABLE `user_test2` (`id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"}},
		}, 2, nil}, //execute 2 correct queries
		{[]*Migration{
			&Migration{ID: "1", Up: []string{"CREATE TABLE `user_test` (`id` mediumint(8) unsigned NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"}},
			&Migration{ID: "2", Up: []string{"this is not a sql query;"}},
		}, 1, errors.New("error applying migration with id 2, query 'this is not a sql query;'. original error : Error 1064: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'this is not a sql query' at line 1")}, //execute 1 correct queries
	}

	for i, c := range cases {
		setUp()
		clean()
		var m = MemoryMigrationSource{
			Migrations: c.migrations,
		}

		applied, err := Exec(db, m, Up)
		if !isSameError(err, c.err) {
			t.Fatalf("[%v] unexpected err: actual\n%v, \nexpected\n%v", i, err, c.err)
		}
		if applied != c.expected {
			t.Errorf("[%v] unexpected length of migrations applied. actual %v, expected %v", i, applied, c.expected)
		}
	}
}

func isSameError(err1, err2 error) bool {
	return fmt.Sprintf("%v", err1) == fmt.Sprintf("%+v", err2)
}
