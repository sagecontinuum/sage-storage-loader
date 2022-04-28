package main

import (
	"log"
	"os"
	"strconv"
	"strings"
)

func getEnv(key string, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func mustGetEnv(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("env var must be defined: %s", key)
	}
	return val
}

func mustParseInt(s string) int {
	num, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("failed to parse %q as int", s)
	}
	return num
}

func mustParseBool(s string) bool {
	switch strings.ToLower(s) {
	case "yes", "true", "1":
		return true
	case "no", "false", "0":
		return false
	default:
		log.Fatalf("failed to parse %q as bool", s)
		return false
	}
}
