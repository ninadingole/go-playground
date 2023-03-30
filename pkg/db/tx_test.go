package db

import (
	"context"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/ninadingole/go-playground/pkg/apptest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

// ------------------------------ UNIT Test ------------------------------

func Test_Unit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		txfunc    func(tx *TxWrap) error
		setup     func(mock sqlmock.Sqlmock)
		wantErr   bool
		wantPanic bool
	}{
		{
			name: "success path",
			txfunc: func(tx *TxWrap) error {
				return nil
			},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectCommit()
			},
		},
		{
			name: "failure path",
			txfunc: func(tx *TxWrap) error {
				return errors.New("some error")
			},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback()
			},
			wantErr: true,
		},
		{
			name: "panic",
			txfunc: func(tx *TxWrap) error {
				panic("some panic")
				return nil
			},
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectRollback()
			},
			wantPanic: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New()
			require.NoError(t, err)

			dbx := sqlx.NewDb(db, "sqlmock")

			test.setup(mock)

			// Wrap the function in a defer to catch panics
			// and assert that the panic is not nil.
			defer func() {
				if test.wantPanic {
					require.NotNil(t, recover())
				}
				stats := dbx.Stats()
				require.Equal(t, 0, stats.InUse)
				require.Equal(t, 0, stats.MaxOpenConnections)
			}()

			err = InTx(context.Background(), dbx, test.txfunc)

			require.Equal(t, test.wantErr, err != nil)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// ---------------------------------- INTEGRATION TEST -------------------------------------
type Employee struct {
	ID   int64  `db:"id"`
	Name string `db:"name"`
}

func Test_Integration(t *testing.T) {
	pg, err := apptest.StartTestPostgres(t)
	require.NoError(t, err)

	_, err = pg.DB.Exec("CREATE TABLE IF NOT EXISTS employee (id serial PRIMARY KEY, name varchar(25) NOT NULL)")
	require.NoError(t, err)

	tests := []struct {
		name      string
		fn        func(tx *TxWrap) error
		wantErr   bool
		wantPanic bool
	}{
		{
			name: "success path",
			fn: func(tx *TxWrap) error {
				_, err := tx.Exec("INSERT INTO employee (name) VALUES ('John Doe')")
				return err
			},
		},
		{
			name: "failure path",
			fn: func(tx *TxWrap) error {
				var employee Employee
				err := tx.Get(&employee, "SELECT * FROM employee WHERE id = $1", 100)
				return err
			},
			wantErr: true,
		},
		{
			name: "panic",
			fn: func(tx *TxWrap) error {
				panic("some panic")

				return nil
			},
			wantPanic: true,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {

			// Wrap the function in a defer to catch panics
			// and assert that the panic is not nil.
			defer func() {
				if test.wantPanic {
					require.NotNil(t, recover())
				}
				stats := pg.DB.Stats()
				require.Equal(t, 0, stats.InUse)
				require.Equal(t, 0, stats.MaxOpenConnections)
			}()

			err = InTx(context.Background(), pg.DB, test.fn)

			require.Equal(t, test.wantErr, err != nil)
		})
	}
}
