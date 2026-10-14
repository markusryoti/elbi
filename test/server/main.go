package main

import (
	"log/slog"
	"net/http"
	"os"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logger.Info("got connection")
		w.Write([]byte("hello world"))
		logger.Info("wrote result")
	})

	logger.Info("starting server", "port", 8080)

	http.ListenAndServe(":8080", mux)
}
