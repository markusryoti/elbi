package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	port := flag.String("port", "8080", "port number to run on")

	flag.Parse()

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		logger.Info("got connection")
		w.Write([]byte("hello world"))
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		logger.Info("healthcheck", "status", "ok")
		w.WriteHeader(http.StatusOK)
	})

	logger.Info("starting server", "port", port)

	err := http.ListenAndServe(fmt.Sprintf(":%s", *port), mux)
	if err != nil {
		logger.Error("error from server listen", "err", err)
	}
}
