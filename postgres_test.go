package postgres

import (
	"log"
	"testing"
)

const (
	connectionString = "host=localhost port=5432 dbname=postgres user=postgres password=postgres sslmode=disable"
)

// TestPostgresDB tests the functionality of the PostgresDB type.
func TestPostgresDB(t *testing.T) {
	db, err := NewPostgresDB(connectionString)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	usersTable := Table{
		Name: "users",
		Columns: []TableColumn{
			{Name: "id", Type: "SERIAL PRIMARY KEY"},
			{Name: "name", Type: "TEXT NOT NULL"},
			{Name: "age", Type: "INT"},
		},
	}

	t.Run("CreateTable", func(t *testing.T) {
		err := db.CreateTable(usersTable)
		if err != nil {
			t.Fatalf("Failed to create table: %s", err)
		}
	})

	t.Run("InsertRow", func(t *testing.T) {
		// Insert a row with correct values
		err := db.InsertRow(usersTable.Name, 1, "John Doe", 30)
		if err != nil {
			t.Fatalf("Failed to insert row: %s", err)
		}
	})

	t.Run("SelectRows", func(t *testing.T) {
		rows, err := db.SelectRows(usersTable.Name, "age > $1", 25)
		if err != nil {
			t.Fatalf("Failed to select rows: %s", err)
		}
		defer rows.Close()

		var count int
		for rows.Next() {
			count++
		}

		if count != 1 {
			t.Fatalf("Expected 1 row, got %d", count)
		}
	})

	t.Run("DeleteRows", func(t *testing.T) {
		// Delete rows where age is greater than 20
		result, err := db.DeleteRows(usersTable.Name, "age > $1", 20)
		if err != nil {
			t.Fatalf("Failed to delete rows: %s", err)
		}

		rowsAffected, err := result.RowsAffected()
		if err != nil {
			t.Fatalf("Failed to get rows affected: %s", err)
		}

		if rowsAffected != 1 {
			t.Fatalf("Expected 1 row to be deleted, got %d", rowsAffected)
		}
	})

	t.Run("DropTable", func(t *testing.T) {
		err := db.DropTable(usersTable.Name)
		if err != nil {
			t.Fatalf("Failed to drop table: %s", err)
		}
	})
}
