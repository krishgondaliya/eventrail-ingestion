package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
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

func main() {
	ctx := context.Background()

	pgDSN := os.Getenv("POSTGRES_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")

	pgPool, err := pgxpool.New(ctx, pgDSN)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pgPool.Close()

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	defer redisClient.Close()

	// --------------------
	// Health Check
	// --------------------
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		pgStatus := "ok"
		if err := pgPool.Ping(ctx); err != nil {
			pgStatus = "error"
		}

		redisStatus := "ok"
		if err := redisClient.Ping(ctx).Err(); err != nil {
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
	// POST /events (idempotent)
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
			log.Printf("failed to persist event: %v", err)
			http.Error(w, "failed to persist event", http.StatusInternalServerError)
			return
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
			 FROM events
			 WHERE id = $1`,
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

	log.Println("EventRail API starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func nilIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
