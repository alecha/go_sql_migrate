package sqlmigrate

var tableName = "database_migrations"

//SetMigrationsTableName sets the table name for the applied migrations
func SetMigrationsTableName(name string) {
	if name != "" {
		tableName = name
	}
}
