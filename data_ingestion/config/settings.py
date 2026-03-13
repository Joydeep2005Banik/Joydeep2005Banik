"""Configuration settings for the Global Ontology Engine data ingestion system."""

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "300"))

DOMAIN_TOPICS = {
    "geopolitics": "raw.geopolitics",
    "economics": "raw.economics",
    "defense": "raw.defense",
    "technology": "raw.technology",
    "climate": "raw.climate",
    "society": "raw.society",
}

DOMAIN_KEYWORDS = {
    "geopolitics": [
        "geopolitics", "diplomacy", "foreign policy", "international relations",
        "sanctions", "treaty", "bilateral", "sovereignty",
    ],
    "economics": [
        "economy", "GDP", "inflation", "trade", "fiscal", "monetary",
        "stock market", "interest rate", "recession",
    ],
    "defense": [
        "defense", "military", "armed forces", "missile", "navy",
        "air force", "army", "security", "warfare",
    ],
    "technology": [
        "technology", "AI", "artificial intelligence", "semiconductor",
        "quantum computing", "cybersecurity", "5G", "space tech",
    ],
    "climate": [
        "climate", "global warming", "carbon emissions", "renewable energy",
        "sustainability", "deforestation", "sea level", "weather",
    ],
    "society": [
        "society", "education", "healthcare", "demographics", "migration",
        "urbanization", "poverty", "public policy",
    ],
}

RSS_FEEDS = {
    "geopolitics": [
        "https://feeds.reuters.com/Reuters/worldNews",
        "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    ],
    "economics": [
        "https://feeds.reuters.com/reuters/businessNews",
        "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
    ],
    "defense": [
        "https://feeds.reuters.com/Reuters/worldNews",
    ],
    "technology": [
        "https://feeds.reuters.com/reuters/technologyNews",
        "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml",
    ],
    "climate": [
        "https://feeds.reuters.com/reuters/environment",
        "https://rss.nytimes.com/services/xml/rss/nyt/Climate.xml",
    ],
    "society": [
        "https://feeds.reuters.com/Reuters/worldNews",
    ],
}

API_SOURCES = {
    "economics": [
        {
            "name": "World Bank Indicators",
            "url": "https://api.worldbank.org/v2/country/IND/indicator/NY.GDP.MKTP.CD",
            "params": {"format": "json", "per_page": "10"},
        },
    ],
}
