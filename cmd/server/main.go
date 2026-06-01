package main

import (
	"log/slog"
	"os"

	"github.com/b9uu/goqueue/internal/config"
	"github.com/jmoiron/sqlx"
)

func main() {
	cfg := config.Load()
	slog.Info("Connecting to db", "dsn", cfg.DatabseURL)
	db, err := sqlx.Connect("postgres", cfg.DatabseURL)
	if err != nil {
		slog.Error("Failed to connect to database", "err", err)
		os.Exit(1)
	}

}
