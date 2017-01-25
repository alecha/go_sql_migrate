package sqlmigrate

//MigrationSource is the interface for retrieving the migrations to be applied
type MigrationSource interface {
	// Finds the migrations.
	// The resulting slice of migrations should be sorted by Id.
	FindMigrations() ([]*Migration, error)
}

//MemoryMigrationSource returns the migrations passed as parameter in the constructor
type MemoryMigrationSource struct {
	Migrations []*Migration
}

//FindMigrations returns the migrations to be applied
func (m MemoryMigrationSource) FindMigrations() ([]*Migration, error) {
	return m.Migrations, nil
}
