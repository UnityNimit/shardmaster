package router

import (
	"strconv"
	"strings"
)

type QueryKind uint8

const (
	QueryPointSelect QueryKind = iota
	QueryPointUpsert
	QueryPointDelete
	QueryScatterGather
	QueryCountAggregate
	QueryAdminShowShards
	QueryAdminShowBuckets
	QueryAdminShowCDC
	QueryAdminShowHotspots
	QueryAdminShowStats
	QueryAdminShowQueries
	QueryAdminExplainShard
	QueryAdminRebalance
	QueryAdminVDiff
	QuerySystemCatalog
)

type ClassifiedQuery struct {
	Kind           QueryKind
	RawSQL         string
	HasShardKey    bool
	UserID         int64
	UserKey        string
	Name           string
	Email          string
	RegionFilter   string
	BalanceCents   int64
	Limit          int
	TargetShards   uint32
}

// ClassifySQL parses a SQL statement with fast string scanning (zero regex overhead).
func ClassifySQL(sql string) ClassifiedQuery {
	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	upper := strings.ToUpper(trimmed)

	// Handle psql startup / system catalog introspection queries
	if strings.HasPrefix(upper, "SET ") ||
		strings.HasPrefix(upper, "SELECT PG_CATALOG") ||
		strings.HasPrefix(upper, "SELECT VERSION()") ||
		strings.HasPrefix(upper, "SELECT CURRENT_") ||
		strings.HasPrefix(upper, "BEGIN") ||
		strings.HasPrefix(upper, "COMMIT") {
		return ClassifiedQuery{Kind: QuerySystemCatalog, RawSQL: trimmed}
	}

	if strings.HasPrefix(upper, "SHOW SHARDS") || strings.HasPrefix(upper, "SHOW TOPOLOGY") {
		return ClassifiedQuery{Kind: QueryAdminShowShards, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW BUCKETS") {
		return ClassifiedQuery{Kind: QueryAdminShowBuckets, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW CDC") || strings.HasPrefix(upper, "SHOW WORKFLOW") || strings.HasPrefix(upper, "SHOW REPLICATION") {
		return ClassifiedQuery{Kind: QueryAdminShowCDC, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW HOTSPOT") || strings.HasPrefix(upper, "SHOW EWMA") {
		return ClassifiedQuery{Kind: QueryAdminShowHotspots, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW STATS") || strings.HasPrefix(upper, "SHOW MEMORY") || strings.HasPrefix(upper, "SHOW TELEMETRY") {
		return ClassifiedQuery{Kind: QueryAdminShowStats, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "SHOW QUERIES") || strings.HasPrefix(upper, "SHOW HELP") || strings.HasPrefix(upper, "SHOW COMMANDS") {
		return ClassifiedQuery{Kind: QueryAdminShowQueries, RawSQL: trimmed}
	}
	if strings.HasPrefix(upper, "EXPLAIN SHARD") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if !ok {
			uid = 42
			ukey = "42"
		}
		return ClassifiedQuery{
			Kind:        QueryAdminExplainShard,
			RawSQL:      trimmed,
			HasShardKey: true,
			UserID:      uid,
			UserKey:     ukey,
		}
	}
	if strings.HasPrefix(upper, "REBALANCE") {
		target := uint32(5)
		fields := strings.Fields(upper)
		for _, f := range fields {
			if n, err := strconv.Atoi(f); err == nil && n >= 2 && n <= 64 {
				target = uint32(n)
				break
			}
		}
		return ClassifiedQuery{Kind: QueryAdminRebalance, RawSQL: trimmed, TargetShards: target}
	}
	if strings.HasPrefix(upper, "RUN VDIFF") || strings.HasPrefix(upper, "SHOW VDIFF") {
		return ClassifiedQuery{Kind: QueryAdminVDiff, RawSQL: trimmed}
	}

	if strings.HasPrefix(upper, "INSERT") || strings.HasPrefix(upper, "UPDATE") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if !ok {
			uid = 42
			ukey = "42"
		}
		return ClassifiedQuery{
			Kind:         QueryPointUpsert,
			RawSQL:       trimmed,
			HasShardKey:  true,
			UserID:       uid,
			UserKey:      ukey,
			Name:         extractQuotedField(trimmed, "name", "User_"+ukey),
			Email:        extractQuotedField(trimmed, "email", "user_"+ukey+"@gmail.com"),
			BalanceCents: 250000,
		}
	}

	if strings.HasPrefix(upper, "DELETE") {
		uid, ukey, ok := extractUserIDClause(trimmed)
		if ok {
			return ClassifiedQuery{
				Kind:        QueryPointDelete,
				RawSQL:      trimmed,
				HasShardKey: true,
				UserID:      uid,
				UserKey:     ukey,
			}
		}
	}

	if strings.Contains(upper, "COUNT(") {
		return ClassifiedQuery{Kind: QueryCountAggregate, RawSQL: trimmed}
	}

	// Check if point query with `user_id = X`
	if uid, ukey, ok := extractUserIDClause(trimmed); ok {
		return ClassifiedQuery{
			Kind:        QueryPointSelect,
			RawSQL:      trimmed,
			HasShardKey: true,
			UserID:      uid,
			UserKey:     ukey,
		}
	}

	// Non-key query -> Distributed Scatter-Gather + Streaming K-Way Merge (Pillar 2)
	limit := extractLimit(upper, 10)
	emailLike := extractLikePattern(trimmed, "email")
	regionEq := extractQuotedField(trimmed, "region", "")

	return ClassifiedQuery{
		Kind:         QueryScatterGather,
		RawSQL:       trimmed,
		HasShardKey:  false,
		Email:        emailLike,
		RegionFilter: regionEq,
		Limit:        limit,
	}
}

func extractUserIDClause(sql string) (int64, string, bool) {
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, "user_id")
	if idx != -1 {
		rest := strings.TrimSpace(sql[idx+len("user_id"):])
		if strings.HasPrefix(rest, "=") {
			rest = strings.TrimSpace(rest[1:])
			token := readToken(rest)
			token = strings.Trim(token, "'\" ")
			tokenClean := strings.TrimPrefix(strings.ToLower(token), "user_")
			if id, err := strconv.ParseInt(tokenClean, 10, 64); err == nil {
				return id, strconv.FormatInt(id, 10), true
			}
			if token != "" {
				return 123, token, true
			}
		}
	}
	// Check VALUES (123, ...)
	vIdx := strings.Index(lower, "values")
	if vIdx != -1 {
		rest := sql[vIdx+6:]
		openParen := strings.IndexByte(rest, '(')
		if openParen != -1 {
			inner := rest[openParen+1:]
			comma := strings.IndexAny(inner, ",)")
			if comma != -1 {
				firstVal := strings.Trim(inner[:comma], "'\" ")
				firstValClean := strings.TrimPrefix(strings.ToLower(firstVal), "user_")
				if id, err := strconv.ParseInt(firstValClean, 10, 64); err == nil {
					return id, strconv.FormatInt(id, 10), true
				}
			}
		}
	}
	return 0, "", false
}

func extractLikePattern(sql, col string) string {
	lower := strings.ToLower(sql)
	idx := strings.Index(lower, col)
	if idx == -1 {
		return ""
	}
	rest := sql[idx+len(col):]
	q1 := strings.IndexByte(rest, '\'')
	if q1 == -1 {
		return ""
	}
	q2 := strings.IndexByte(rest[q1+1:], '\'')
	if q2 == -1 {
		return ""
	}
	return rest[q1+1 : q1+1+q2]
}

func extractQuotedField(sql, col, fallback string) string {
	val := extractLikePattern(sql, col)
	if val == "" {
		return fallback
	}
	return val
}

func extractLimit(upperSQL string, defaultLimit int) int {
	idx := strings.Index(upperSQL, "LIMIT ")
	if idx == -1 {
		return defaultLimit
	}
	rest := strings.TrimSpace(upperSQL[idx+6:])
	tok := readToken(rest)
	if n, err := strconv.Atoi(tok); err == nil && n > 0 && n <= 500 {
		return n
	}
	return defaultLimit
}

func readToken(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == ' ' || ch == '\t' || ch == '\n' || ch == ';' || ch == ')' || ch == ',' {
			break
		}
		b.WriteByte(ch)
	}
	return b.String()
}
