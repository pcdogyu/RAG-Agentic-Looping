package main

import (
	"context"
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

	"github.com/pcdogyu/RAG-Agentic-Looping/backend-go/internal/marketadapter"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "check the local market-adapter health endpoint")
	flag.Parse()
	address := env("ADAPTER_ADDRESS", "0.0.0.0") + ":" + env("ADAPTER_PORT", "8091")
	if *healthcheck {
		if err := checkHealth(address); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	timeout := time.Duration(envInt("MARKET_PROVIDER_TIMEOUT_SECONDS", 90)) * time.Second
	provider := marketadapter.NewProvider(&http.Client{Timeout: timeout}, marketadapter.ProviderConfig{
		SinaUniverseURL: env("SINA_UNIVERSE_URL", ""), TencentChinaURL: env("TENCENT_CN_KLINE_URL", ""),
		TencentHKURL: env("TENCENT_HK_KLINE_URL", ""), FundamentalsURL: env("EASTMONEY_FUNDAMENTALS_URL", ""),
		NewsURL: env("EASTMONEY_FAST_NEWS_URL", ""),
	})
	server := &http.Server{
		Addr: address, Handler: marketadapter.NewHandler(provider), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: timeout + 5*time.Second, WriteTimeout: timeout + 5*time.Second, IdleTimeout: 60 * time.Second,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	slog.Info("Go market adapter listening", "address", address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("market adapter stopped", "error", err)
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
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + net.JoinHostPort(host, port) + "/health")
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

func envInt(name string, fallback int) int {
	var value int
	if _, err := fmt.Sscan(env(name, ""), &value); err == nil && value > 0 {
		return value
	}
	return fallback
}
