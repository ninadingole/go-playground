package apptest

import (
	"context"
	"database/sql"
	"github.com/jmoiron/sqlx"
)

type Subscription struct {
	ID         int64        `db:"id"`
	Status     string       `db:"status"`
	CanceledAt sql.NullTime `db:"canceled_at"`
}

type srepo struct {
}

func (r *srepo) GetSubscription(tx *sqlx.Tx, id int64) (Subscription, error) {
	var sub Subscription
	err := tx.Get(&sub, "SELECT * FROM subscription WHERE id = $1", id)
	if err != nil {
		return sub, err
	}

	return sub, nil
}

func (r *srepo) CancelSubscription(tx *sqlx.Tx, id int64) (Subscription, error) {
	var sub Subscription
	err := tx.Get(&sub, "UPDATE subscription SET canceled_at = NOW() WHERE id = $1 RETURNING *", id)
	if err != nil {
		return sub, err
	}

	return sub, nil
}

type Service struct {
	db   *sqlx.DB
	repo *srepo
}

func NewService(db *sqlx.DB, repo *srepo) *Service {
	return &Service{repo: repo, db: db}
}

func (s *Service) CancelSubscription(ctx context.Context, id int64) (*Subscription, error) {
	tx, err := s.db.BeginTxx(ctx, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
	}()

	sub, err := s.repo.GetSubscription(tx, id)
	if err != nil {
		return nil, err
	}

	if sub.Status != "active" {
		return &sub, nil
	}

	if sub.CanceledAt.Valid {
		return &sub, nil
	}

	sub, err = s.repo.CancelSubscription(tx, id)
	if err != nil {
		return nil, err
	}

	err = tx.Commit()

	return &sub, err
}
