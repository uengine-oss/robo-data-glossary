"""ROBO Data Glossary 환경변수 설정"""

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

project_root = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=project_root / ".env")


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str = field(default_factory=lambda: os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"))
    user: str = field(default_factory=lambda: os.getenv("NEO4J_USER", "neo4j"))
    password: str = field(default_factory=lambda: os.getenv("NEO4J_PASSWORD", "neo4j"))
    database: str = "neo4j"


@dataclass(frozen=True)
class LLMConfig:
    api_base: str = field(default_factory=lambda: os.getenv("LLM_API_BASE", "https://api.openai.com/v1"))
    api_key: str = field(default_factory=lambda: os.getenv("LLM_API_KEY", ""))
    model: str = field(default_factory=lambda: os.getenv("LLM_MODEL", "gpt-4.1"))
    max_tokens: int = field(default_factory=lambda: int(os.getenv("LLM_MAX_TOKENS", "32768")))
    reasoning_effort: str = field(default_factory=lambda: os.getenv("LLM_REASONING_EFFORT", "medium"))
    is_custom: bool = field(default_factory=lambda: os.getenv("IS_CUSTOM_LLM", "").lower() == "true")
    company_name: Optional[str] = field(default_factory=lambda: os.getenv("COMPANY_NAME"))
    temperature: float = field(default_factory=lambda: float(os.getenv("LLM_TEMPERATURE", "0.2")))
    reasoning_models: frozenset = field(default_factory=lambda: frozenset({
        "gpt-5", "o1", "o1-pro", "o1-mini", "o1-preview", "o3", "o3-mini", "o4-mini"
    }))
    embedding_model: str = field(default_factory=lambda: os.getenv("EMBEDDING_MODEL", "text-embedding-3-small"))
    cache_enabled: bool = field(default_factory=lambda: os.getenv("LLM_CACHE_ENABLED", "true").lower() == "true")
    cache_db_path: str = field(default_factory=lambda: os.getenv("LLM_CACHE_DB", ".llm_cache.db"))


@dataclass(frozen=True)
class PathConfig:
    base_dir: str = field(default_factory=lambda: str(Path(__file__).resolve().parents[2]))


@dataclass(frozen=True)
class GlossaryConfig:
    neo4j: Neo4jConfig = field(default_factory=Neo4jConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    path: PathConfig = field(default_factory=PathConfig)

    version: str = "2.0.0"
    api_prefix: str = "/robo"
    host: str = field(default_factory=lambda: os.getenv("HOST", "0.0.0.0"))


@lru_cache(maxsize=1)
def get_settings() -> GlossaryConfig:
    return GlossaryConfig()


settings = get_settings()
