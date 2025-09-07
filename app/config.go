package main

import "io"

// Configuration and Options

// DatabaseConfig holds database configuration options
type DatabaseConfig struct {
	PageCacheSize   int
	MaxConcurrency  int
	ReadTimeout     int // milliseconds
	ValidationMode  ValidationLevel
	EnableProfiling bool
}

// ValidationLevel defines validation strictness
type ValidationLevel int

const (
	ValidationNone ValidationLevel = iota
	ValidationBasic
	ValidationStrict
)

// DatabaseOption represents a functional option for database configuration
type DatabaseOption func(*DatabaseConfig)

// WithPageCacheSize sets the page cache size
func WithPageCacheSize(size int) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.PageCacheSize = size
	}
}

// WithMaxConcurrency sets the maximum number of concurrent operations
func WithMaxConcurrency(max int) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.MaxConcurrency = max
	}
}

// WithReadTimeout sets the read timeout in milliseconds
func WithReadTimeout(timeout int) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.ReadTimeout = timeout
	}
}

// WithValidation sets the validation level
func WithValidation(level ValidationLevel) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.ValidationMode = level
	}
}

// WithProfiling enables or disables profiling
func WithProfiling(enabled bool) DatabaseOption {
	return func(cfg *DatabaseConfig) {
		cfg.EnableProfiling = enabled
	}
}

// DefaultDatabaseConfig returns the default configuration
func DefaultDatabaseConfig() *DatabaseConfig {
	return &DatabaseConfig{
		PageCacheSize:   100,
		MaxConcurrency:  10,
		ReadTimeout:     5000, // 5 seconds
		ValidationMode:  ValidationBasic,
		EnableProfiling: false,
	}
}

// Resource Management

// ResourceManager handles cleanup of multiple resources
type ResourceManager struct {
	resources []io.Closer
	cleaners  []func() error
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		resources: make([]io.Closer, 0),
		cleaners:  make([]func() error, 0),
	}
}

// Add adds a closeable resource to be managed
func (rm *ResourceManager) Add(resource io.Closer) {
	rm.resources = append(rm.resources, resource)
}

// AddCleaner adds a custom cleanup function
func (rm *ResourceManager) AddCleaner(cleaner func() error) {
	rm.cleaners = append(rm.cleaners, cleaner)
}

// Close closes all managed resources in reverse order (LIFO)
func (rm *ResourceManager) Close() error {
	var lastErr error

	// Run custom cleaners first (LIFO)
	for i := len(rm.cleaners) - 1; i >= 0; i-- {
		if err := rm.cleaners[i](); err != nil {
			lastErr = err
		}
	}

	// Close resources (LIFO)
	for i := len(rm.resources) - 1; i >= 0; i-- {
		if err := rm.resources[i].Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}