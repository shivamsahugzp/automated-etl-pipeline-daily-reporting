# analytics_50.md
# Generated: 2025-10-15 22:10:27
# Author: Shivam Sahu

# Configuration file for analytics system
DEBUG = True
VERSION = "1.0.0"
ENVIRONMENT = "development"

# Database configuration
DATABASE_URL = "postgresql://user:password@localhost:5432/analytics"

# API configuration
API_BASE_URL = "https://api.analytics.com"
API_TIMEOUT = 30
API_RETRIES = 3

# Logging configuration
LOG_LEVEL = "INFO"
LOG_FILE = "logs/analytics.log"
LOG_MAX_SIZE = "10MB"
LOG_BACKUP_COUNT = 5

# Cache configuration
CACHE_TTL = 300
CACHE_MAX_SIZE = 1000

# Security configuration
SECRET_KEY = "your-secret-key-here"
ALLOWED_HOSTS = ["localhost", "127.0.0.1"]

# Feature flags
ENABLE_REAL_TIME = True
ENABLE_EXPORT = True
ENABLE_ALERTS = True
