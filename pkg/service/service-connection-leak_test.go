package service

import (
	"context"
	"github.com/ninadingole/go-playground/pkg/apptest"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_ConnectionLeak(t *testing.T) {
	pg, err := apptest.StartTestPostgres(t)
	require.NoError(t, err)

	_, err = pg.DB.Exec("CREATE TABLE IF NOT EXISTS subscription (id serial PRIMARY KEY, status varchar(25) NOT NULL, canceled_at timestamp NULL)")
	require.NoError(t, err)

	_, err = pg.DB.Exec("INSERT INTO subscription (status, canceled_at) VALUES ('active', NULL)")
	require.NoError(t, err)

	_, err = pg.DB.Exec("INSERT INTO subscription (status, canceled_at) VALUES ('canceled', '2023-02-02 01:00:00')")
	require.NoError(t, err)

	subscription, err := NewService(pg.DB, &srepo{}).CancelSubscription(context.Background(), 2)
	require.NoError(t, err)

	stats := pg.DB.Stats()
	require.Equal(t, 1, stats.InUse, "expected no connections in use")
	require.Equal(t, 1, stats.MaxOpenConnections, "expected no max open connection")

	require.Equal(t, "canceled", subscription.Status)
	require.Equal(t, "2023-02-02 01:00:00 +0000 +0000", subscription.CanceledAt.Time.String())
}
