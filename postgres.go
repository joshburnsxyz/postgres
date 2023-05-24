package postgres

import (
	"database/sql"
	"fmt"
	"strings"
	_ "github.com/lib/pq"
)

// PostgresDB represents a PostgreSQL database connection.
type PostgresDB struct {
	db *sql.DB
}

// TableColumn represents a column in a database table.
type TableColumn struct {
	Name string
	Type string
}

// Table represents a database table.
type Table struct {
	Name    string
	Columns []TableColumn
}

// NewPostgresDB creates a new instance of PostgresDB with the provided connection string.
func NewPostgresDB(connectionString string) (*PostgresDB, error) {
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	return &PostgresDB{db: db}, nil
}

// Close closes the database connection.
func (p *PostgresDB) Close() error {
	return p.db.Close()
}

// CreateTable creates a table in the database based on the provided Table.
func (p *PostgresDB) CreateTable(table Table) error {
	columnDefinitions := make([]string, len(table.Columns))
	for i, column := range table.Columns {
		columnDefinitions[i] = fmt.Sprintf("%s %s", column.Name, column.Type)
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", table.Name, strings.Join(columnDefinitions, ", "))
	_, err := p.db.Exec(query)
	return err
}

// DropTable drops a table from the database.
func (p *PostgresDB) DropTable(tableName string) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := p.db.Exec(query)
	return err
}

// InsertRow inserts a new row into the table.
func (p *PostgresDB) InsertRow(tableName string, values ...interface{}) error {
	query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", tableName, createPlaceholders(len(values)))
	_, err := p.db.Exec(query, values...)
	return err
}

// SelectRows selects rows from the table based on the provided condition.
func (p *PostgresDB) SelectRows(tableName, condition string, args ...interface{}) (*sql.Rows, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE %s", tableName, condition)
	return p.db.Query(query, args...)
}

// DeleteRows deletes rows from the table based on the provided condition.
func (p *PostgresDB) DeleteRows(tableName, condition string, args ...interface{}) (sql.Result, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE %s", tableName, condition)
	return p.db.Exec(query, args...)
}

// createPlaceholders returns a comma-separated string of placeholders for the given count.
func createPlaceholders(count int) string {
	placeholders := make([]string, count)
	for i := 0; i < count; i++ {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return strings.Join(placeholders, ", ")
}
