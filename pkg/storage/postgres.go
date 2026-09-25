package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresMultiplexer manages non-blocking pgx/v5 connection pools to physical PostgreSQL shards.
type PostgresMultiplexer struct {
	mu    sync.RWMutex
	pools map[uint32]*pgxpool.Pool
}

func NewPostgresMultiplexer() *PostgresMultiplexer {
	return &PostgresMultiplexer{
		pools: make(map[uint32]*pgxpool.Pool),
	}
}

const shardSchemaDDL = `
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    user_key VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(128) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    region VARCHAR(32) NOT NULL,
    balance_cents BIGINT NOT NULL DEFAULT 0,
    bucket_id SMALLINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_users_bucket_id ON users(bucket_id, user_id);

CREATE TABLE IF NOT EXISTS _shardmaster_cdc (
    lsn BIGSERIAL PRIMARY KEY,
    bucket_id SMALLINT NOT NULL,
    op SMALLINT NOT NULL,
    user_id BIGINT NOT NULL,
    payload JSONB NOT NULL,
    recorded_at_us BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cdc_bucket_lsn ON _shardmaster_cdc(bucket_id, lsn);
`

// TryConnectAndBootstrap attempts a fast non-blocking connection to a Docker PostgreSQL node
// and initializes the shard schema if reachable.
func (pm *PostgresMultiplexer) TryConnectAndBootstrap(shard *PhysicalShard) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(shard.DSN)
	if err != nil {
		return false
	}
	cfg.MaxConns = 16
	cfg.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return false
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return false
	}

	if _, err := pool.Exec(ctx, shardSchemaDDL); err != nil {
		pool.Close()
		return false
	}

	pm.mu.Lock()
	pm.pools[shard.ShardID] = pool
	shard.PostgresUp = true
	pm.mu.Unlock()
	return true
}

func (pm *PostgresMultiplexer) GetPool(shardID uint32) (*pgxpool.Pool, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	p, ok := pm.pools[shardID]
	return p, ok
}

func (pm *PostgresMultiplexer) CloseAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for id, p := range pm.pools {
		p.Close()
		delete(pm.pools, id)
	}
}

func FormatShardDSN(shardID uint32) string {
	port := 5432 + int(shardID)
	return fmt.Sprintf("postgres://postgres:postgres@localhost:%d/shard_%d?sslmode=disable", port, shardID)
}
