import os
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """
    Validates and maps system environment variables into typed configuration properties.
    Loads configurations from Kubernetes ConfigMaps, Secrets, or a local .env file.
    """

    # 1. Core Global Infrastructure Variables
    APP_ENV: str = "development"  # "development" | "production" | "testing"
    LOG_LEVEL: str = "INFO"  # "DEBUG" | "INFO" | "WARNING" | "ERROR"

    # 2. Decoupled Network Variables
    BACKEND_HOST: str = "0.0.0.0"
    BACKEND_PORT: int = 8080

    # 3. Structural Broker API Integration Framework
    BROKER_ENV: str = "paper"  # "paper" | "live"
    BROKER_API_KEY: str = "development_mock_api_key_placeholder"
    BROKER_SECRET_KEY: str = "development_mock_secret_key_placeholder"

    # 4. Global Baseline Strategy Risk Allocations
    TARGET_SYMBOL: str = "AAPL"
    DEFAULT_QTY: int = 50
    RSI_PERIOD: int = 14
    RSI_OVERBOUGHT: float = 70.0
    RSI_OVERSOLD: float = 30.0
    ML_CONFIDENCE_THRESHOLD: float = 0.65

    # 5. Pydantic Parsing Configurations
    # Instructs Pydantic to look for a local environment configuration file first.
    # If variables exist natively in the environment (e.g. injected via K8s pods),
    # the environment variables instantly override these .env properties.
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"  # Gracefully drop unrelated variables in the environment
    )


# Instantiate a global configuration object to import across files
settings = AppSettings()
