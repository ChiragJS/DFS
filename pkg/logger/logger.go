package logger

import (
	"log/slog"
	"os"
)

var (
	// Default logger instance
	Log *slog.Logger
)

func init() {
	// Initialize with JSON handler for structured logging
	// Can be changed to TextHandler for human-readable output
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Set minimum log level
	})
	Log = slog.New(handler)
	slog.SetDefault(Log)
}

// Info logs at INFO level
func Info(msg string, args ...any) {
	Log.Info(msg, args...)
}

// Debug logs at DEBUG level
func Debug(msg string, args ...any) {
	Log.Debug(msg, args...)
}

// Warn logs at WARN level
func Warn(msg string, args ...any) {
	Log.Warn(msg, args...)
}

// Error logs at ERROR level
func Error(msg string, args ...any) {
	Log.Error(msg, args...)
}

// With returns a logger with additional context
func With(args ...any) *slog.Logger {
	return Log.With(args...)
}
