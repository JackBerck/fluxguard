package limiter

import "log/slog"

// Logger is the interface FluxGuard uses for structured logging.
// Any [*slog.Logger] satisfies this interface out of the box. Pass nil or
// omit the option to disable all library-level logging.
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// nopLogger discards every log record.
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

// resolveLogger returns l if non-nil, otherwise a no-op logger so callers
// never need to nil-check before logging.
func resolveLogger(l Logger) Logger {
	if l != nil {
		return l
	}
	return nopLogger{}
}

// SlogLogger wraps a [*slog.Logger] to satisfy [Logger].
// Use it when you want to plug FluxGuard into an existing slog pipeline:
//
//	limiter.NewTokenBucket(store, 10, 2,
//	    limiter.WithLogger(limiter.NewSlogLogger(slog.Default())),
//	)
type SlogLogger struct{ l *slog.Logger }

// NewSlogLogger returns a [Logger] backed by l.
func NewSlogLogger(l *slog.Logger) *SlogLogger { return &SlogLogger{l: l} }

func (s *SlogLogger) Debug(msg string, args ...any) { s.l.Debug(msg, args...) }
func (s *SlogLogger) Info(msg string, args ...any)  { s.l.Info(msg, args...) }
func (s *SlogLogger) Warn(msg string, args ...any)  { s.l.Warn(msg, args...) }
func (s *SlogLogger) Error(msg string, args ...any) { s.l.Error(msg, args...) }
