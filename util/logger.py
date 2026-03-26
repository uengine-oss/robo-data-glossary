"""ROBO Analyzer 로깅 유틸리티

일관된 로그 포맷과 컨텍스트 로깅을 제공합니다.

사용법:
    from util.logger import get_logger, log_context
    
    logger = get_logger(__name__)
    logger.info("분석 시작", file="test.java", line=100)
    
    with log_context(file="test.java", phase="LLM"):
        logger.info("작업 수행")
"""

import logging
import sys
from contextvars import ContextVar
from typing import Any

# 컨텍스트 변수
_log_context: ContextVar[dict[str, Any]] = ContextVar("log_context", default={})


class ContextFilter(logging.Filter):
    """로그에 컨텍스트 정보를 자동 추가하는 필터"""
    
    def filter(self, record: logging.LogRecord) -> bool:
        ctx = _log_context.get()
        for key, value in ctx.items():
            setattr(record, key, value)
        return True


class RoboFormatter(logging.Formatter):
    """ROBO Analyzer 전용 로그 포맷터
    
    출력 형식:
        2024-01-01 12:00:00 [INFO] module_name: 메시지 | key=value
    """
    
    def format(self, record: logging.LogRecord) -> str:
        # 기본 메시지
        msg = super().format(record)
        
        # 추가 키워드 인자가 있으면 붙이기
        extra_parts = []
        skip_attrs = {
            "name", "msg", "args", "created", "filename", "funcName",
            "levelname", "levelno", "lineno", "module", "msecs",
            "pathname", "process", "processName", "relativeCreated",
            "stack_info", "exc_info", "exc_text", "thread", "threadName",
            "message", "asctime",
        }
        
        for key, value in record.__dict__.items():
            if key not in skip_attrs and not key.startswith("_"):
                extra_parts.append(f"{key}={value}")
        
        if extra_parts:
            msg = f"{msg} | {', '.join(extra_parts)}"
        
        return msg


def setup_logging(level: int = logging.INFO) -> None:
    """애플리케이션 로깅 설정
    
    Args:
        level: 로그 레벨 (기본: INFO)
    """
    # UTF-8 인코딩 설정
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
            sys.stderr.reconfigure(encoding="utf-8")
        except Exception:
            pass
    
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    
    # 기존 핸들러 제거
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 새 핸들러 추가
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.setFormatter(RoboFormatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    handler.addFilter(ContextFilter())
    root_logger.addHandler(handler)
    
    # 서드파티 로거 레벨 조정
    for name in ["neo4j", "httpx", "httpcore", "urllib3"]:
        logging.getLogger(name).setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """로거 인스턴스 반환
    
    Args:
        name: 로거 이름 (보통 __name__)
        
    Returns:
        logging.Logger 인스턴스
    """
    return logging.getLogger(name)



