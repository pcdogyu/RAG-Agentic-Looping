package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/config"
	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/jobs"
	"github.com/redis/go-redis/v9"
)

type Server struct {
	cfg              config.Config
	db               *pgxpool.Pool
	redis            *redis.Client
	router           http.Handler
	nativeOperations []operation
}

const totalContractOperations = 81

func New(cfg config.Config, db *pgxpool.Pool, redisClient *redis.Client) (*Server, error) {
	s := &Server{cfg: cfg, db: db, redis: redisClient}
	s.nativeOperations = s.operations()
	if cfg.Environment == "production" && len(s.nativeOperations) != totalContractOperations {
		return nil, fmt.Errorf("Go cutover blocked: %d of %d contract operations are native", len(s.nativeOperations), totalContractOperations)
	}
	r := chi.NewRouter()
	r.Use(requestLog, recoverer, etagMiddleware)
	r.Get("/go/health", s.goHealth)
	r.Get("/go/migration-status", s.migrationStatus)
	for _, item := range s.nativeOperations {
		r.MethodFunc(item.Method, item.Path, item.Handler)
	}

	r.HandleFunc("/*", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	s.router = r
	return s, nil
}

func (s *Server) migrationStatus(w http.ResponseWriter, _ *http.Request) {
	native := len(s.nativeOperations)
	writeJSON(w, http.StatusOK, map[string]any{
		"total_operations": totalContractOperations, "native_operations": native,
		"remaining_operations": totalContractOperations - native,
		"cutover_ready":        native == totalContractOperations,
		"native_operation_ids": operationIDs(s.nativeOperations),
		"worker_runtime":       jobs.RuntimeStatus(),
	})
}

func (s *Server) Handler() http.Handler { return s.router }

func (s *Server) goHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 750*time.Millisecond)
	defer cancel()
	database := s.db.Ping(ctx) == nil
	redisOK := s.redis.Ping(ctx).Err() == nil
	status := "ok"
	if !database || !redisOK {
		status = "degraded"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": status, "database": database, "redis": redisOK,
		"implementation": "go", "as_of": time.Now().UTC(),
	})
}

func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		next.ServeHTTP(w, r)
		slog.Info("http request", "method", r.Method, "path", r.URL.Path, "elapsed", time.Since(started))
	})
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if value := recover(); value != nil {
				slog.Error("request panic", "value", value)
				writeError(w, http.StatusInternalServerError, "internal server error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

type bufferedWriter struct {
	header http.Header
	status int
	body   strings.Builder
}

func (w *bufferedWriter) Header() http.Header    { return w.header }
func (w *bufferedWriter) WriteHeader(status int) { w.status = status }
func (w *bufferedWriter) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.body.Write(value)
}

func etagMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
			next.ServeHTTP(w, r)
			return
		}
		buffer := &bufferedWriter{header: make(http.Header)}
		next.ServeHTTP(buffer, r)
		for key, values := range buffer.header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		body := buffer.body.String()
		if buffer.status >= 200 && buffer.status < 300 && body != "" {
			tag := weakETag([]byte(body))
			w.Header().Set("ETag", tag)
			w.Header().Set("Cache-Control", "private, no-cache")
			if r.Header.Get("If-None-Match") == tag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		status := buffer.status
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}

func validationError(w http.ResponseWriter, location, message string) {
	writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": []map[string]any{{
		"type": "value_error", "loc": []string{"query", location}, "msg": message,
	}}})
}

func isNoRows(err error) bool { return errors.Is(err, context.Canceled) }
