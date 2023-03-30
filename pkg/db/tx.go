package db

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
)

// TxWrap is a wrapper around sqlx.Tx that adds a context
// and redirects calls to methods like Get, Select to GetContext and SelectContext
// with the context it wraps.
type TxWrap struct {
	tx  *sqlx.Tx        // underlying transaction
	ctx context.Context // context to use for all calls
}

// Get is a wrapper around sqlx.Tx.GetContext
// that uses the context it wraps.
func (tx *TxWrap) Get(dest interface{}, query string, args ...interface{}) error {
	return tx.tx.GetContext(tx.ctx, dest, query, args...)
}

// Select is a wrapper around sqlx.Tx.SelectContext
// that uses the context it wraps.
func (tx *TxWrap) Select(dest interface{}, query string, args ...interface{}) error {
	return tx.tx.SelectContext(tx.ctx, dest, query, args...)
}

func (tx *TxWrap) Exec(query string, args ...any) (sql.Result, error) {
	return tx.tx.ExecContext(tx.ctx, query, args...)
}

// InTx executes a function in a transaction.
// If the function returns an error, the transaction is rolled back.
// If the function panics, the transaction is rolled back and the panic is re-raised.
// If the function returns nil, the transaction is committed.
func InTx(ctx context.Context, db *sqlx.DB, txFunc func(*TxWrap) error) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}

	txWrap := &TxWrap{
		tx:  tx,
		ctx: ctx,
	}

	defer func() {
		if p := recover(); p != nil {
			_ = txWrap.tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = txWrap.tx.Rollback()
			return
		}
		err = txWrap.tx.Commit()
	}()

	return txFunc(txWrap)
}
