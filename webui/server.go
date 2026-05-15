// Package webui serves a single-page local web UI for emptybucket. Credentials
// are kept only in memory for the lifetime of the process; nothing is written
// to disk. The page receives live updates via Server-Sent Events.
package webui

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/nikosubra/emptybucket-portable/runner"
)

//go:embed index.html
var indexHTML []byte

// Server holds the SSE subscriber set and the run lifecycle.
type Server struct {
	mu          sync.Mutex
	subscribers map[chan runner.Event]struct{}
	running     bool
	lastResult  *runner.Result
}

func New() *Server {
	return &Server{subscribers: make(map[chan runner.Event]struct{})}
}

func (s *Server) subscribe() chan runner.Event {
	ch := make(chan runner.Event, 256)
	s.mu.Lock()
	s.subscribers[ch] = struct{}{}
	s.mu.Unlock()
	return ch
}

func (s *Server) unsubscribe(ch chan runner.Event) {
	s.mu.Lock()
	delete(s.subscribers, ch)
	s.mu.Unlock()
	close(ch)
}

func (s *Server) broadcast(ev runner.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.subscribers {
		select {
		case ch <- ev:
		default: // drop if subscriber lags
		}
	}
}

// Handler returns the mux for the web UI on the given context. Cancellation
// of ctx will stop any in-flight deletion run.
func (s *Server) Handler(ctx context.Context) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var req runner.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := req.Validate(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		s.mu.Lock()
		if s.running {
			s.mu.Unlock()
			http.Error(w, "a run is already in progress", http.StatusConflict)
			return
		}
		s.running = true
		s.lastResult = nil
		s.mu.Unlock()

		go func() {
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			events := make(chan runner.Event, 256)
			done := make(chan runner.Result, 1)
			go func() {
				done <- runner.Run(runCtx, req, events)
			}()
			for ev := range events {
				s.broadcast(ev)
			}
			res := <-done
			s.mu.Lock()
			s.running = false
			s.lastResult = &res
			s.mu.Unlock()
		}()

		w.WriteHeader(http.StatusAccepted)
		fmt.Fprintln(w, `{"status":"started"}`)
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		ch := s.subscribe()
		defer s.unsubscribe(ch)

		// Heartbeat so clients keep the connection open through proxies.
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				fmt.Fprintf(w, ": ping\n\n")
				flusher.Flush()
			case ev, ok := <-ch:
				if !ok {
					return
				}
				b, err := json.Marshal(ev)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Kind, b)
				flusher.Flush()
			}
		}
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"running":    s.running,
			"lastResult": s.lastResult,
		})
	})
	return mux
}

// Serve starts a blocking HTTP server on addr.
func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{Addr: addr, Handler: s.Handler(ctx)}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	return srv.ListenAndServe()
}
