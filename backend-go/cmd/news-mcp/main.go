package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/newsmcp"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "check the local news MCP health endpoint")
	flag.Parse()
	address := env("NEWS_MCP_ADDRESS", "0.0.0.0:8080")
	if *healthcheck {
		if err := checkHealth(address); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	mux := http.NewServeMux()
	mux.Handle("/mcp", newsmcp.NewServer(newsmcp.Config{
		BaseURL:      env("NEWS_SEARCH_API_BASE", "https://ai.6551.io"),
		DefaultToken: os.Getenv("OPENNEWS_TOKEN"),
		APIClient:    &http.Client{Timeout: 30 * time.Second},
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "news-mcp", "implementation": "go"})
	})
	server := &http.Server{
		Addr: address, Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 35 * time.Second, WriteTimeout: 35 * time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("Go news MCP listening", "address", address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("news MCP stopped", "error", err)
		os.Exit(1)
	}
}

func checkHealth(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	response, err := (&http.Client{Timeout: 2 * time.Second}).Get("http://" + net.JoinHostPort(host, port) + "/health")
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("health returned %s", response.Status)
	}
	return nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
