package apptest

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"io"
	"math/rand"
	"net"
	"testing"
	"time"
)

type PGConn struct {
	t          *testing.T
	dcPool     *dockertest.Pool
	dbResource *dockertest.Resource
	hostname   string
	port       string
	DB         *sqlx.DB
	DSN        string
	TimeZone   *time.Location
}

// StartTestPostgres starts a postgres container and returns a sqlx connection to it.
func StartTestPostgres(t *testing.T) (*PGConn, error) {
	t.Helper()

	// Postgres uses Etc/UTC location so dates need to be created in same timezone for equality
	timeZone, err := time.LoadLocation("Etc/UTC")
	if err != nil {
		return nil, errors.Wrap(err, "could not load timezone")
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, errors.Wrap(err, "could not connect to docker")
	}

	pgConn := &PGConn{
		t:        t,
		dcPool:   pool,
		TimeZone: timeZone,
	}

	err = pgConn.startPostgresContainer()
	if err != nil {
		return nil, errors.Wrap(err, "could not start postgres container")
	}

	return pgConn, nil
}

func (pg *PGConn) startPostgresContainer() error {
	port, err := getFreePort()
	if err != nil {
		return errors.Wrap(err, "could not get free port")
	}

	hostName := withRandomSuffix("postgres-db")
	hostPort := fmt.Sprintf("%d/tcp", port)

	dbResource, err := pg.dcPool.RunWithOptions(&dockertest.RunOptions{
		Name:       hostName,
		Hostname:   hostName,
		Repository: "postgres",
		Tag:        "14",
		Env:        []string{"POSTGRES_PASSWORD=postgres", "POSTGRES_DB=test"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432/tcp": {{HostIP: "", HostPort: hostPort}},
		},
		ExposedPorts: []string{hostPort},
	})
	if err != nil {
		return errors.Wrap(err, "could not start resource")
	}

	pg.t.Cleanup(func() {
		if err := dbResource.Close(); err != nil {
			pg.t.Logf("could not close resource: %v", err)
		}
	})

	dockerHostPort := dbResource.GetPort("5432/tcp")
	dsn := GetDSN(dockerHostPort, "test")

	err = pg.dcPool.Retry(func() error {
		db, err := sql.Open("postgres", dsn)
		if err != nil {
			return err
		}

		dbx := sqlx.NewDb(db, "postgres")

		defer db.Close()

		return dbx.DB.Ping()
	})

	if err != nil && errors.Is(err, io.EOF) {
		return errors.Wrap(err, "could not connect to docker")
	}

	pg.dbResource = dbResource
	pg.hostname = hostName
	pg.port = dockerHostPort
	pg.DSN = dsn

	db, err := sql.Open("postgres", pg.DSN)
	if err != nil {
		return errors.Wrap(err, "could not open db")
	}

	dbx := sqlx.NewDb(db, "postgres")

	pg.t.Cleanup(func() {
		if err := db.Close(); err != nil {
			pg.t.Logf("could not close db: %v", err)
		}
	})

	pg.DB = dbx

	return nil
}

// NewConnection returns a new connection to the database.
func (pg *PGConn) NewConnection(t *testing.T, db string) *PGConn {
	t.Helper()

	_, err := pg.DB.Exec(fmt.Sprintf("CREATE DATABASE %s", db))
	require.NoError(t, err)

	dbx, err := sqlx.Open("postgres", GetDSN(pg.port, db))
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, dbx.Close())
	})

	return &PGConn{
		DB:         dbx,
		dbResource: pg.dbResource,
		TimeZone:   pg.TimeZone,
	}
}

func GetDSN(port string, dbName string) string {
	return fmt.Sprintf(
		"postgres://postgres:postgres@localhost:%s/%s?sslmode=disable",
		port, dbName,
	)
}

func getFreePort() (port int, err error) {
	var a *net.TCPAddr

	if a, err = net.ResolveTCPAddr("tcp", "localhost:0"); err == nil {
		var l *net.TCPListener

		if l, err = net.ListenTCP("tcp", a); err == nil {
			defer l.Close()

			return l.Addr().(*net.TCPAddr).Port, nil
		}
	}

	return
}

func withRandomSuffix(prefix string) string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprintf("%s-%d", prefix, r.Int63()) //nolint:gosec
}
