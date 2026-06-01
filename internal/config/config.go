package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabseURL        string
	Port              string
	WorkerConcurrency int
	PollInterval      time.Duration
}

func Load() *Config {
	return &Config{
		DatabseURL:        getEnv("DATABASE_URL", "postgres://goqueue:secret@172.23.16.1:5432/goqueue?sslmode=disable"),
		Port:              getEnv("PORT", "8080"),
		WorkerConcurrency: getEnvInt("WORKER_CONCURRENCY", 10),
		PollInterval:      getEnvDuration("POLL_INTERVAL", 2*time.Second),
	}
}
func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}
