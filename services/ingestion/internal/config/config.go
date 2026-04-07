package config

import (
	"os"
	"strconv"
)

type Config struct {
	Port            string
	PostgresDSN     string
	RedisAddr       string
	KafkaBrokers    []string
	APIKey          string
	MigrationsPath  string
}

func Load() Config {
	return Config{
		Port:           getEnv("PORT", "8080"),
		PostgresDSN:    getEnv("POSTGRES_DSN", "postgres://carrier:carrier_secret@localhost:5432/carrier_opt?sslmode=disable"),
		RedisAddr:      getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:   []string{getEnv("KAFKA_BROKERS", "localhost:9092")},
		APIKey:         getEnv("API_KEY", "local-upload-key"),
		MigrationsPath: getEnv("MIGRATIONS_PATH", "migrations"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func GetASRThreshold() float64 {
	v, _ := strconv.ParseFloat(getEnv("ASR_THRESHOLD", "0.40"), 64)
	return v
}
