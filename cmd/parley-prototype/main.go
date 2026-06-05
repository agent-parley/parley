package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/agent-parley/parley/internal/manager/web"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := getenv("PARLEY_ADDR", "127.0.0.1:8080")
	renderer, err := web.NewRenderer()
	if err != nil {
		fmt.Fprintf(os.Stderr, "parley-prototype: %v\n", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/assets/", http.StripPrefix("/", http.FileServer(http.FS(web.Embedded))))
	mux.HandleFunc("/prototype", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method", http.StatusMethodNotAllowed)
			return
		}
		query := r.URL.Query()
		writePage(w, renderer, "prototype.html", web.NewPrototypeDataWithOptions(web.PrototypeOptions{
			RunID: query.Get("run"),
			Tab:   query.Get("tab"),
			View:  query.Get("view"),
			Mock:  query.Get("mock"),
		}))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.Redirect(w, r, "/prototype", http.StatusSeeOther)
	})

	server := &http.Server{Addr: addr, Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		fmt.Fprintf(os.Stderr, "Parley prototype listening on http://%s/prototype\n", addr)
		err := server.ListenAndServe()
		if err == http.ErrServerClosed {
			err = nil
		}
		errCh <- err
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(os.Stderr, "parley-prototype: shutdown: %v\n", err)
			os.Exit(1)
		}
		if err := <-errCh; err != nil {
			fmt.Fprintf(os.Stderr, "parley-prototype: %v\n", err)
			os.Exit(1)
		}
	case err := <-errCh:
		if err != nil {
			fmt.Fprintf(os.Stderr, "parley-prototype: %v\n", err)
			os.Exit(1)
		}
	}
}

func writePage(w http.ResponseWriter, renderer web.Renderer, name string, data any) {
	html, err := renderer.ExecutePage(name, data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(html)))
	_, _ = w.Write([]byte(html))
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
