package sqlmigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

//Migration is a sql migration
type Migration struct {
	ID   string
	Up   []string
	Down []string
}

//AppliedMigration is an applied migration stored in a table
type AppliedMigration struct {
	ID        string
	AppliedAt time.Time
}

// Exec executes a set of migrations
// Returns the number of applied migrations.
func Exec(db *sql.DB, m MigrationSource, dir MigrationDirection) (int, error) {
	if dir != Up {
		return 0, errors.New("only migration up direction is supported")
	}

	//init table
	if err := createTableIfNotExists(db); err != nil {
		return 0, err
	}

	//get all the migrations
	migrations, err := m.FindMigrations()
	if err != nil {
		return 0, err
	}

	//get the applied migrations
	appliedMigrations, err := getAppliedMigrations(db)
	if err != nil {
		return 0, err
	}

	//understand which are the migrations to apply
	migrationsToApply := getMigrationsToApply(migrations, appliedMigrations)

	//apply each migration
	var appliedMigrationsCount = 0
	for i := range migrationsToApply {
		tx, _ := db.Begin()

		for q := range migrationsToApply[i].Up {
			//apply each sub-query
			_, err := tx.Exec(migrationsToApply[i].Up[q])
			if err != nil {
				tx.Rollback()
				return appliedMigrationsCount, newApplyMigrationError(migrationsToApply[i].ID, migrationsToApply[i].Up[q], err)
			}
		}

		//write that this migration is applied
		query := fmt.Sprintf("INSERT INTO %v (`id`,`applied_at`) VALUES (?,?)", tableName)
		_, err := tx.Exec(query, migrationsToApply[i].ID, time.Now())
		if err != nil {
			tx.Rollback()
			return appliedMigrationsCount, newApplyMigrationError(migrationsToApply[i].ID, query, err)
		}

		if err := tx.Commit(); err != nil {
			return appliedMigrationsCount, err
		}
		appliedMigrationsCount++
	}

	return appliedMigrationsCount, nil
}

//ApplyMigrationError is an error while applying a migration
type ApplyMigrationError struct {
	ID    string
	Query string
	Err   error
}

//Error returns a string representation of the ApplyMigrationError
func (a ApplyMigrationError) Error() string {
	return fmt.Sprintf("error applying migration with id %v, query '%v'. original error : %v", a.ID, a.Query, a.Err)
}

func newApplyMigrationError(id string, query string, err error) ApplyMigrationError {
	return ApplyMigrationError{
		ID:    id,
		Query: query,
		Err:   err,
	}
}

func getAppliedMigrations(db *sql.DB) ([]AppliedMigration, error) {
	var query = fmt.Sprintf("SELECT * FROM %s", tableName)
	var appliedMigrations []AppliedMigration
	res, err := db.Query(query)
	if err != nil {
		return appliedMigrations, err
	}
	defer res.Close()
	for res.Next() {
		var current AppliedMigration
		if err := res.Scan(&current.ID, &current.AppliedAt); err != nil {
			return appliedMigrations, err
		}
		appliedMigrations = append(appliedMigrations, current)
	}
	return appliedMigrations, nil
}

func createTableIfNotExists(db *sql.DB) error {
	var query = fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v (`id` varchar(255) NOT NULL,`applied_at` datetime DEFAULT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8;", tableName)
	_, err := db.Exec(query)
	return err
}

//iterate over all the migrations, and add to the list to the migrations to apply if it's not applied yet (in same order)
func getMigrationsToApply(allMigrations []*Migration, appliedMigrations []AppliedMigration) []Migration {
	var migrationsToApply []Migration
	for i := range allMigrations {
		var shouldApplyMigration = true
		for k := range appliedMigrations {
			if allMigrations[i].ID == appliedMigrations[k].ID {
				shouldApplyMigration = false
			}
		}
		if shouldApplyMigration {
			migrationsToApply = append(migrationsToApply, *allMigrations[i])
		}
	}
	return migrationsToApply
}

//MigrationDirection is a migration direction
type MigrationDirection uint8

//these are the possible migration direction
const (
	Up MigrationDirection = iota + 1
	Down
)
