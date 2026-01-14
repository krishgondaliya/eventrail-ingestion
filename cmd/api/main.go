package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

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

	log.Println("EventRail API starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
