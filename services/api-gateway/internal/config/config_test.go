package config

import "testing"

func TestLoad_DefaultsToDevelopment(t *testing.T) {
	t.Setenv("ENVIRONMENT", "")

	cfg := Load()

	if cfg.IsProduction {
		t.Fatal("expected default environment to be non-production")
	}
}

func TestLoad_ProductionOnlyWhenExplicit(t *testing.T) {
	t.Setenv("ENVIRONMENT", "production")

	cfg := Load()

	if !cfg.IsProduction {
		t.Fatal("expected production environment to enable production mode")
	}
}

func TestLoad_ProductionEnvironmentIsCaseInsensitive(t *testing.T) {
	t.Setenv("ENVIRONMENT", "Production")

	cfg := Load()

	if !cfg.IsProduction {
		t.Fatal("expected production environment check to be case-insensitive")
	}
}

func TestLoad_DevelopmentIsNotProduction(t *testing.T) {
	t.Setenv("ENVIRONMENT", "development")

	cfg := Load()

	if cfg.IsProduction {
		t.Fatal("expected development environment to disable production mode")
	}
}
