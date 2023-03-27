package service

import (
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/ninadingole/go-playground/pkg/db"
)

// ------------------------------ Repository ------------------------------

type txRepo struct {
}

// GetSubscription is a repository method that does not leak connections
// it uses *TxWrap to execute the query inside the transaction.
// it uses the context to cancel the transaction if the context is canceled
// but the context is inside the *TxWrap and not exposed to the service
func (r *txRepo) GetSubscription(tx *db.TxWrap, id int64) (Subscription, error) {
	var sub Subscription
	err := tx.Get(&sub, "SELECT * FROM subscription WHERE id = $1", id)
	if err != nil {
		return sub, err
	}

	return sub, nil
}

// CancelSubscription cancels the subscription
// It uses *TxWrap to execute the query inside the transaction
func (r *txRepo) CancelSubscription(tx *db.TxWrap, id int64) (Subscription, error) {
	var sub Subscription
	err := tx.Get(&sub, "UPDATE subscription SET canceled_at = NOW(), status='canceled' WHERE id = $1 RETURNING *", id)
	if err != nil {
		return sub, err
	}

	return sub, nil
}

// ------------------------------ Service ------------------------------

type txService struct {
	db   *sqlx.DB
	repo *txRepo
}

func NewTxService(db *sqlx.DB, repo *txRepo) *txService {
	return &txService{repo: repo, db: db}
}

// CancelSubscriptionWithoutLeak is a service method that does not leak connections
// it uses InTx helper to wrap the transaction
func (s *txService) CancelSubscriptionWithoutLeak(ctx context.Context, id int64) (*Subscription, error) {
	var sub Subscription
	var err error

	err = db.InTx(ctx, s.db, func(tx *db.TxWrap) error {
		sub, err = s.repo.GetSubscription(tx, id)
		if err != nil {
			return err
		}

		if sub.Status != "active" {
			return nil
		}

		if sub.CanceledAt.Valid {
			return nil
		}

		sub, err = s.repo.CancelSubscription(tx, id)
		if err != nil {
			return err
		}

		return nil
	})

	return &sub, err
}
