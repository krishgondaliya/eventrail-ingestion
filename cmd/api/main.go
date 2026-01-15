package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

const (
	EventStream   = "eventrail.events"
	ConsumerGroup = "eventrail.cg"
	DLQStream     = "eventrail.events.dlq"
	RetryZSet     = "eventrail.events.retry" // delayed retry scheduler (ZSET)

	DefaultWorker      = "api-1"
	DefaultMaxRetries  = 5
	DefaultBaseBackoff = 500 * time.Millisecond
)

type CreateEventRequest struct {
	EventType string          `json:"event_type"`
	Source    string          `json:"source"`
	Payload   json.RawMessage `json:"payload"`
}

type CreateEventResponse struct {
	ID string `json:"id"`
}

type Event struct {
	ID        string          `json:"id"`
	EventType string          `json:"event_type"`
	Source    string          `json:"source"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
}

type ReplayRequest struct {
	From      string `json:"from"`       // RFC3339
	To        string `json:"to"`         // RFC3339
	Source    string `json:"source"`     // optional
	EventType string `json:"event_type"` // optional
	Limit     int    `json:"limit"`      // optional, default 1000
}

type ReplayResponse struct {
	Published int `json:"published"`
}

type SetGroupCursorRequest struct {
	Group   string `json:"group"`   // optional, default ConsumerGroup
	StartID string `json:"start_id"`// e.g. "0" or "0-0" or "$"
}

func main() {
	ctx := context.Background()

	pgDSN := os.Getenv("POSTGRES_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")

	consumer := strings.TrimSpace(os.Getenv("CONSUMER_NAME"))
	if consumer == "" {
		consumer = DefaultWorker
	}

	maxRetries := envInt("MAX_RETRIES", DefaultMaxRetries)
	baseBackoff := envDuration("BASE_BACKOFF_MS", DefaultBaseBackoff)

	pgPool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pgPool.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	if err := ensureConsumerGroup(ctx, redisClient); err != nil {
		log.Fatalf("failed to ensure consumer group: %v", err)
	}

	// Worker reads stream, processes, acks, schedules retries, moves to DLQ
	go startStreamWorker(redisClient, consumer, maxRetries, baseBackoff)

	// Retry pump pulls due items from ZSET and republishes them to the stream
	go retryPump(redisClient)

	// --------------------
	// Health Check
	// --------------------
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		hctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		pgStatus := "ok"
		if err := pgPool.Ping(hctx); err != nil {
			pgStatus = "error"
		}

		redisStatus := "ok"
		if err := redisClient.Ping(hctx).Err(); err != nil {
			redisStatus = "error"
		}

		status := "ok"
		if pgStatus != "ok" || redisStatus != "ok" {
			status = "degraded"
			w.WriteHeader(http.StatusServiceUnavailable)
		}

		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(
			`{"status":"` + status + `","postgres":"` + pgStatus + `","redis":"` + redisStatus + `"}`,
		))
	})

	// --------------------
	// POST /events (idempotent + publish)
	// --------------------
	http.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		idempotencyKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))

		var req CreateEventRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		if req.EventType == "" || req.Source == "" || len(req.Payload) == 0 {
			http.Error(w, "event_type, source, and payload are required", http.StatusBadRequest)
			return
		}

		var eventID string
		err := pgPool.QueryRow(
			context.Background(),
			`INSERT INTO events (event_type, source, payload, idempotency_key)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (source, idempotency_key)
			 WHERE idempotency_key IS NOT NULL
			 DO UPDATE SET source = EXCLUDED.source
			 RETURNING id`,
			req.EventType,
			req.Source,
			req.Payload,
			nilIfEmpty(idempotencyKey),
		).Scan(&eventID)

		if err != nil {
			http.Error(w, "failed to persist event", http.StatusInternalServerError)
			return
		}

		_, err = redisClient.XAdd(context.Background(), &redis.XAddArgs{
			Stream: EventStream,
			Values: map[string]interface{}{
				"event_id":   eventID,
				"event_type": req.EventType,
				"source":     req.Source,
				"retry":      "0",
				"created_at": time.Now().UTC().Format(time.RFC3339),
			},
		}).Result()

		if err != nil {
			log.Printf("failed to publish event %s to stream: %v", eventID, err)
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(CreateEventResponse{ID: eventID})
	})

	// --------------------
	// GET /events/{id}
	// --------------------
	http.HandleFunc("/events/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		id := strings.TrimPrefix(r.URL.Path, "/events/")
		if id == "" {
			http.Error(w, "event id required", http.StatusBadRequest)
			return
		}

		var evt Event
		err := pgPool.QueryRow(
			context.Background(),
			`SELECT id, event_type, source, payload, created_at
			 FROM events WHERE id = $1`,
			id,
		).Scan(
			&evt.ID,
			&evt.EventType,
			&evt.Source,
			&evt.Payload,
			&evt.CreatedAt,
		)

		if err == pgx.ErrNoRows {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "failed to fetch event", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(evt)
	})

	// --------------------
	// POST /replay (backfill by time range from Postgres into Redis Stream)
	// --------------------
	http.HandleFunc("/replay", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req ReplayRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		if req.From == "" || req.To == "" {
			http.Error(w, "from and to are required (RFC3339)", http.StatusBadRequest)
			return
		}

		fromT, err := time.Parse(time.RFC3339, req.From)
		if err != nil {
			http.Error(w, "from must be RFC3339", http.StatusBadRequest)
			return
		}
		toT, err := time.Parse(time.RFC3339, req.To)
		if err != nil {
			http.Error(w, "to must be RFC3339", http.StatusBadRequest)
			return
		}

		limit := req.Limit
		if limit <= 0 || limit > 5000 {
			limit = 1000
		}

		query := `
			SELECT id, event_type, source, created_at
			FROM events
			WHERE created_at >= $1 AND created_at <= $2`
		args := []interface{}{fromT, toT}

		if req.Source != "" {
			query += " AND source = $3"
			args = append(args, req.Source)
		}

		if req.EventType != "" {
			if req.Source != "" {
				query += " AND event_type = $4"
			} else {
				query += " AND event_type = $3"
			}
			args = append(args, req.EventType)
		}

		query += " ORDER BY created_at ASC LIMIT " + strconv.Itoa(limit)

		rows, err := pgPool.Query(context.Background(), query, args...)
		if err != nil {
			http.Error(w, "failed to query events", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		published := 0
		for rows.Next() {
			var id, eventType, source string
			var createdAt time.Time
			if err := rows.Scan(&id, &eventType, &source, &createdAt); err != nil {
				http.Error(w, "failed to read events", http.StatusInternalServerError)
				return
			}

			_, err := redisClient.XAdd(context.Background(), &redis.XAddArgs{
				Stream: EventStream,
				Values: map[string]interface{}{
					"event_id":   id,
					"event_type": eventType,
					"source":     source,
					"retry":      "0",
					"created_at": createdAt.UTC().Format(time.RFC3339),
					"replay":     "1",
				},
			}).Result()
			if err != nil {
				log.Printf("replay publish failed for %s: %v", id, err)
				continue
			}
			published++
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ReplayResponse{Published: published})
	})

	// --------------------
	// POST /consumer-groups/set-cursor (consumer replay)
	// Sets the group cursor so consumers can reprocess history.
	// --------------------
	http.HandleFunc("/consumer-groups/set-cursor", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req SetGroupCursorRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		group := strings.TrimSpace(req.Group)
		if group == "" {
			group = ConsumerGroup
		}

		startID := strings.TrimSpace(req.StartID)
		if startID == "" {
			http.Error(w, "start_id is required", http.StatusBadRequest)
			return
		}

		if err := redisClient.XGroupSetID(context.Background(), EventStream, group, startID).Err(); err != nil {
			http.Error(w, "failed to set group cursor", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	})

	log.Println("EventRail API starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func ensureConsumerGroup(ctx context.Context, rdb *redis.Client) error {
	err := rdb.XGroupCreateMkStream(ctx, EventStream, ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func startStreamWorker(rdb *redis.Client, consumer string, maxRetries int, baseBackoff time.Duration) {
	ctx := context.Background()
	log.Printf("stream worker started (group=%s consumer=%s)", ConsumerGroup, consumer)

	for {
		res, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    ConsumerGroup,
			Consumer: consumer,
			Streams:  []string{EventStream, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()

		if err != nil {
			if err == redis.Nil {
				continue
			}
			log.Printf("XREADGROUP error: %v", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		for _, stream := range res {
			for _, msg := range stream.Messages {
				retry := parseRetry(msg.Values["retry"])

				if err := processMessage(msg); err != nil {
					// failure path
					nextRetry := retry + 1
					if nextRetry > maxRetries {
						moveToDLQ(ctx, rdb, msg, err)
						_ = rdb.XAck(ctx, EventStream, ConsumerGroup, msg.ID).Err()
						continue
					}

					delay := backoffDelay(baseBackoff, nextRetry)
					if err := scheduleRetry(ctx, rdb, msg, nextRetry, delay); err != nil {
						// If scheduling fails, keep it unacked so it stays pending.
						log.Printf("schedule retry failed (msg=%s): %v", msg.ID, err)
						continue
					}

					// Once scheduled, ack original so it doesn't sit in pending forever
					_ = rdb.XAck(ctx, EventStream, ConsumerGroup, msg.ID).Err()
					continue
				}

				// success
				log.Printf("processed event stream_id=%s event_id=%v type=%v source=%v retry=%d",
					msg.ID, msg.Values["event_id"], msg.Values["event_type"], msg.Values["source"], retry)

				if err := rdb.XAck(ctx, EventStream, ConsumerGroup, msg.ID).Err(); err != nil {
					log.Printf("XACK error: %v", err)
				}
			}
		}
	}
}

// processMessage is where delivery work happens.
// For testing retries, if event_type == "force.fail" we fail intentionally.
func processMessage(msg redis.XMessage) error {
	et, _ := msg.Values["event_type"].(string)
	if et == "force.fail" {
		return errors.New("forced failure for testing")
	}
	return nil
}

func scheduleRetry(ctx context.Context, rdb *redis.Client, msg redis.XMessage, nextRetry int, delay time.Duration) error {
	// We store the full Values as JSON in a ZSET member so we can re-publish later.
	values := msg.Values
	values["retry"] = strconv.Itoa(nextRetry)
	values["original_stream_id"] = msg.ID

	b, err := json.Marshal(values)
	if err != nil {
		return err
	}

	due := time.Now().Add(delay).UnixMilli()
	return rdb.ZAdd(ctx, RetryZSet, redis.Z{
		Score:  float64(due),
		Member: string(b),
	}).Err()
}

func retryPump(rdb *redis.Client) {
	ctx := context.Background()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now().UnixMilli()

		members, err := rdb.ZRangeByScore(ctx, RetryZSet, &redis.ZRangeBy{
			Min:    "-inf",
			Max:    strconv.FormatInt(now, 10),
			Offset: 0,
			Count:  50,
		}).Result()
		if err != nil || len(members) == 0 {
			continue
		}

		for _, m := range members {
			// Remove first to avoid double-publish on crash loops
			removed, _ := rdb.ZRem(ctx, RetryZSet, m).Result()
			if removed == 0 {
				continue
			}

			var values map[string]interface{}
			if err := json.Unmarshal([]byte(m), &values); err != nil {
				continue
			}

			_, err := rdb.XAdd(ctx, &redis.XAddArgs{
				Stream: EventStream,
				Values: values,
			}).Result()
			if err != nil {
				// If republish fails, put it back with a short delay
				_ = rdb.ZAdd(ctx, RetryZSet, redis.Z{
					Score:  float64(time.Now().Add(1 * time.Second).UnixMilli()),
					Member: m,
				}).Err()
			}
		}
	}
}

func moveToDLQ(ctx context.Context, rdb *redis.Client, msg redis.XMessage, cause error) {
	values := msg.Values
	values["dlq_at"] = time.Now().UTC().Format(time.RFC3339)
	values["error"] = cause.Error()
	values["original_stream_id"] = msg.ID

	_, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DLQStream,
		Values: values,
	}).Result()
	if err != nil {
		log.Printf("failed to write to DLQ: %v", err)
	}
}

func parseRetry(v interface{}) int {
	s, ok := v.(string)
	if !ok {
		return 0
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0
	}
	return i
}

func backoffDelay(base time.Duration, retry int) time.Duration {
	// Exponential backoff: base * 2^(retry-1), capped
	mult := 1 << (retry - 1)
	d := time.Duration(mult) * base
	if d > 10*time.Second {
		return 10 * time.Second
	}
	return d
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return v
}

// BASE_BACKOFF_MS is an int in milliseconds
func envDuration(key string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	ms, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return time.Duration(ms) * time.Millisecond
}
