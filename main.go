package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/mmcdole/gofeed"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	gitlab "gitlab.com/gitlab-org/api/client-go/v2"
	"go.yaml.in/yaml/v3"
)

type Config struct {
	Feeds    []Feed `yaml:"feeds"`
	Interval int    `yaml:"interval"`
}

type Feed struct {
	ID              string    `yaml:"id"`
	FeedURL         string    `yaml:"feed_url"`
	Name            string    `yaml:"name"`
	GitlabProjectID int       `yaml:"gitlab_project_id"`
	Labels          []string  `yaml:"labels"`
	AddedSince      time.Time `yaml:"added_since"`
	Retroactive     bool      `yaml:"retroactive"`
}

type Env struct {
	RedisURL         string
	RedisPassword    string
	ConfDir          string
	GitlabAPIToken   string
	GitlabAPIBaseURL string
	UseSentinel      bool
}

type metrics struct {
	lastRun        prometheus.Gauge
	issuesCreated  prometheus.Counter
	creationErrors prometheus.Counter
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		lastRun: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "last_run_time",
			Help: "Last Run Time in Unix Seconds",
		}),
		issuesCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "issue_creation_total",
			Help: "The total number of issues created in Gitlab since start-up",
		}),
		creationErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "issue_creation_error_total",
			Help: "The total of failures in creating Gitlab issues since start-up",
		}),
	}
	reg.MustRegister(m.lastRun, m.issuesCreated, m.creationErrors)
	return m
}

func hasExistingGitlabIssue(ctx context.Context, guid string, projectID int, gl *gitlab.Client) (bool, error) {
	opts := &gitlab.SearchOptions{ListOptions: gitlab.ListOptions{PerPage: 100}}
	issues, _, err := gl.Search.IssuesByProject(projectID, guid, opts, gitlab.WithContext(ctx))
	if err != nil {
		return false, fmt.Errorf("searching gitlab for %q: %w", guid, err)
	}
	if len(issues) == 0 {
		return false, nil
	}
	urls := make([]string, 0, len(issues))
	for _, i := range issues {
		urls = append(urls, i.WebURL)
	}
	slog.Info("found existing gitlab issue(s) for guid", "guid", guid, "urls", strings.Join(urls, ", "))
	return true, nil
}

func (f Feed) check(ctx context.Context, rdb *redis.Client, gl *gitlab.Client, m *metrics) {
	fp := gofeed.NewParser()
	rss, err := fp.ParseURLWithContext(f.FeedURL, ctx)
	if err != nil {
		slog.Error("parse feed", "feed", f.Name, "err", err)
		return
	}

	var fresh, seen int
	for _, item := range rss.Items {
		if ctx.Err() != nil {
			return
		}

		found, err := rdb.SIsMember(ctx, f.ID, item.GUID).Result()
		if err != nil {
			slog.Error("redis sismember", "feed", f.Name, "err", err)
			return
		}
		if found {
			seen++
			continue
		}
		fresh++

		itemTime := item.UpdatedParsed
		if itemTime == nil {
			itemTime = item.PublishedParsed
		}
		if itemTime != nil && itemTime.Before(f.AddedSince) {
			slog.Info("skipping item older than added_since",
				"feed", f.Name, "title", item.Title,
				"item_time", itemTime, "added_since", f.AddedSince)
			if err := rdb.SAdd(ctx, f.ID, item.GUID).Err(); err != nil {
				slog.Error("redis sadd", "err", err)
			}
			continue
		}

		exists, err := hasExistingGitlabIssue(ctx, item.GUID, f.GitlabProjectID, gl)
		if err != nil {
			slog.Error("gitlab search", "err", err)
			continue
		}
		if exists {
			if err := rdb.SAdd(ctx, f.ID, item.GUID).Err(); err != nil {
				slog.Error("redis sadd", "err", err)
			}
			continue
		}

		body := item.Description
		if body == "" {
			body = item.Content
		}

		issueTime := time.Now()
		if f.Retroactive && itemTime != nil {
			issueTime = *itemTime
		}

		labels := gitlab.LabelOptions(f.Labels)
		opts := &gitlab.CreateIssueOptions{
			Title:       gitlab.Ptr(item.Title),
			Description: gitlab.Ptr(fmt.Sprintf("%s\n\n%s\n\n---\nGUID: `%s`", body, item.Link, item.GUID)),
			Labels:      &labels,
			CreatedAt:   &issueTime,
		}
		if _, _, err := gl.Issues.CreateIssue(f.GitlabProjectID, opts, gitlab.WithContext(ctx)); err != nil {
			slog.Error("create gitlab issue", "feed", f.Name, "title", item.Title, "err", err)
			m.creationErrors.Inc()
			continue
		}
		if err := rdb.SAdd(ctx, f.ID, item.GUID).Err(); err != nil {
			slog.Error("redis sadd", "title", item.Title, "err", err)
			continue
		}
		m.issuesCreated.Inc()
		slog.Info("created gitlab issue",
			"feed", f.Name, "title", item.Title,
			"project", f.GitlabProjectID,
			"retroactive", f.Retroactive, "issue_time", issueTime)
	}

	slog.Info("checked feed", "feed", f.Name, "new", fresh, "seen", seen)
}

func readConfig(p string) (*Config, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	c := &Config{}
	if err := yaml.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if c.Interval <= 0 {
		return nil, errors.New("interval must be > 0")
	}
	return c, nil
}

func readEnv() (Env, error) {
	required := func(k string) (string, error) {
		v, ok := os.LookupEnv(k)
		if !ok || v == "" {
			return "", fmt.Errorf("missing or empty env var %s", k)
		}
		return v, nil
	}

	var (
		e   Env
		err error
	)
	if e.GitlabAPIBaseURL, err = required("GITLAB_API_BASE_URL"); err != nil {
		return e, err
	}
	if e.GitlabAPIToken, err = required("GITLAB_API_TOKEN"); err != nil {
		return e, err
	}
	if e.ConfDir, err = required("CONFIG_DIR"); err != nil {
		return e, err
	}
	if e.RedisURL, err = required("REDIS_URL"); err != nil {
		return e, err
	}
	// REDIS_PASSWORD must be set but may be empty.
	pw, ok := os.LookupEnv("REDIS_PASSWORD")
	if !ok {
		return e, errors.New("REDIS_PASSWORD must be set (may be empty)")
	}
	e.RedisPassword = pw
	_, e.UseSentinel = os.LookupEnv("USE_SENTINEL")
	return e, nil
}

func newRedis(env Env) *redis.Client {
	if env.UseSentinel {
		return redis.NewFailoverClient(&redis.FailoverOptions{
			SentinelAddrs: []string{env.RedisURL},
			Password:      env.RedisPassword,
			MasterName:    "mymaster",
		})
	}
	return redis.NewClient(&redis.Options{
		Addr:     env.RedisURL,
		Password: env.RedisPassword,
	})
}

func runServer(ctx context.Context, name, addr string, h http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		slog.Info("http server listening", "name", name, "addr", addr)
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errCh <- err
	}()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		return <-errCh
	case err := <-errCh:
		return err
	}
}

func runFeedLoop(ctx context.Context, cfg *Config, rdb *redis.Client, gl *gitlab.Client, m *metrics) {
	interval := time.Duration(cfg.Interval) * time.Second
	t := time.NewTicker(interval)
	defer t.Stop()

	tick := func() {
		slog.Info("running checks", "feeds", len(cfg.Feeds))
		for _, f := range cfg.Feeds {
			if ctx.Err() != nil {
				return
			}
			f.check(ctx, rdb, gl, m)
		}
		m.lastRun.SetToCurrentTime()
	}

	tick()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}

func main() {
	metricsAddr := flag.String("metrics-addr", ":8080", "address for /metrics")
	healthAddr := flag.String("health-addr", ":8081", "address for /healthz")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))

	env, err := readEnv()
	if err != nil {
		slog.Error("env", "err", err)
		os.Exit(1)
	}

	cfg, err := readConfig(filepath.Join(env.ConfDir, "config.yaml"))
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}

	gl, err := gitlab.NewClient(env.GitlabAPIToken, gitlab.WithBaseURL(env.GitlabAPIBaseURL))
	if err != nil {
		slog.Error("gitlab client", "err", err)
		os.Exit(1)
	}

	rdb := newRedis(env)
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		slog.Error("redis ping", "addr", env.RedisURL, "err", err)
		os.Exit(1)
	}
	cancel()
	slog.Info("connected to redis", "addr", env.RedisURL)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := newMetrics(reg)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg}))

	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := rdb.Ping(pingCtx).Err(); err != nil {
			http.Error(w, "redis unreachable", http.StatusServiceUnavailable)
			return
		}
		_, _ = fmt.Fprintln(w, "ok")
	})

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		if err := runServer(ctx, "metrics", *metricsAddr, metricsMux); err != nil {
			slog.Error("metrics server", "err", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := runServer(ctx, "health", *healthAddr, healthMux); err != nil {
			slog.Error("health server", "err", err)
		}
	}()
	go func() {
		defer wg.Done()
		runFeedLoop(ctx, cfg, rdb, gl, m)
	}()

	wg.Wait()
	slog.Info("shutdown complete")
}
