"""LLM 클라이언트 팩토리

OpenAI 호환 API 또는 커스텀 LLM 클라이언트를 생성합니다.

LLM 캐싱:
    LLM_CACHE_ENABLED=true 환경변수로 SQLite 기반 캐싱 활성화
    동일한 프롬프트 호출 시 캐싱된 결과 반환 (테스트 시 유용)

사용법:
    llm = get_llm(api_key="...")
    response = llm.invoke("안녕하세요")
"""

import logging
import os
import requests
import threading
from typing import Any, Dict, List, Optional, Union

from langchain_openai import ChatOpenAI
from langchain_community.cache import SQLiteCache
from langchain_core.globals import set_llm_cache
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain_core.outputs import ChatResult, ChatGeneration

from config.settings import settings


# =============================================================================
# 커스텀 LLM 클라이언트
# =============================================================================

class CustomLLMClient(BaseChatModel):
    """커스텀 LLM API 클라이언트"""
    
    api_key: str
    model: str
    base_url: str
    temperature: float = 0.1
    max_tokens: Optional[int] = None
    timeout: int = 500
    verify_ssl: bool = False
    company_name: Optional[str] = None

    def __init__(
        self,
        api_key: str,
        model: str,
        base_url: str,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        timeout: int = 500,
        verify_ssl: bool = False,
        company_name: Optional[str] = None,
    ):
        super().__init__(
            api_key=api_key.strip(),
            model=model,
            base_url=base_url.rstrip("/"),
            temperature=temperature,
            max_tokens=max_tokens,
            timeout=timeout,
            verify_ssl=verify_ssl,
            company_name=company_name,
        )

    @property
    def _llm_type(self) -> str:
        return "custom-chat-model"

    def _convert_messages(self, messages: List[BaseMessage]) -> List[Dict[str, str]]:
        """메시지 리스트를 API 형식으로 변환"""
        out = []
        for m in messages:
            if isinstance(m, SystemMessage):
                role = "system" if self.model.startswith("gpt-5") else "developer"
            elif isinstance(m, HumanMessage):
                role = "user"
            elif isinstance(m, AIMessage):
                role = "assistant"
            else:
                role = "user"
            out.append({"role": role, "content": m.content})
        return out

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[Union[str, List[str]]] = None
    ) -> ChatResult:
        """LLM API 호출 및 응답 생성"""
        headers = {
            "accept": "*/*",
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": self._convert_messages(messages),
        }

        # 모델별 파라미터 설정
        if self.model.startswith("gpt-5"):
            if self.max_tokens is not None:
                payload["max_completion_tokens"] = self.max_tokens
        else:
            payload["temperature"] = self.temperature
            if self.max_tokens is not None:
                payload["max_tokens"] = self.max_tokens

        if stop:
            payload["stop"] = stop

        resp = requests.post(
            self.base_url,
            headers=headers,
            json=payload,
            timeout=self.timeout,
            verify=self.verify_ssl,
        )

        if not resp.ok:
            try:
                error_data = resp.json()
            except ValueError:
                error_data = resp.text
            print(f"[ERROR] API 호출 실패: status={resp.status_code}, body={error_data}")
            resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "").lower()
        if "application/json" in content_type:
            data = resp.json()
            content = (
                data.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                    .strip()
            )
        else:
            content = resp.text.strip()

        ai_msg = AIMessage(content=content)
        return ChatResult(generations=[ChatGeneration(message=ai_msg)])

    def invoke(
        self,
        prompt_value: Any,
        config: Optional[Dict[str, Any]] = None
    ) -> str:
        """프롬프트 실행 및 응답 반환"""
        text = getattr(prompt_value, "to_string", lambda: str(prompt_value))()

        if config:
            self.temperature = config.get("temperature", self.temperature)
            self.max_tokens = config.get("max_tokens", self.max_tokens)

        messages: List[BaseMessage] = [
            SystemMessage(content="당신은 소스 코드 분석 전문가입니다. 사용자가 요청하는 분석 작업을 정확하게 수행해주세요."),
            HumanMessage(content=text)
        ]

        result = self._generate(messages, stop=config.get("stop") if config else None)
        return result.generations[0].message.content

    def __call__(self, prompt_value: Any, **config: Any) -> str:
        return self.invoke(prompt_value, config)


# =============================================================================
# LLM 캐싱
# =============================================================================

# LLM 캐싱 초기화 (한 번만 실행). 병렬 호출 시 락으로 1회만 초기화·로그 출력.
_cache_initialized = False
_cache_lock = threading.Lock()


def _init_llm_cache():
    """LLM 캐싱 초기화 (멀티스레드에서도 1회만 실행, 로그 1회만 출력)."""
    global _cache_initialized

    if _cache_initialized:
        return
    with _cache_lock:
        if _cache_initialized:
            return
        config = settings.llm
        if config.cache_enabled:
            cache_path = config.cache_db_path
            if not os.path.isabs(cache_path):
                cache_path = os.path.join(settings.path.base_dir, cache_path)
            try:
                set_llm_cache(SQLiteCache(database_path=cache_path))
                logging.info("LLM 캐싱 활성화: %s", cache_path)
            except Exception as e:
                logging.warning("LLM 캐싱 초기화 실패: %s", e)
        else:
            logging.debug("LLM 캐싱 비활성화됨")
        _cache_initialized = True


# =============================================================================
# LLM 클라이언트 팩토리
# =============================================================================

def _is_reasoning_model(model: str) -> bool:
    """추론 모델 여부 확인"""
    model_lower = model.lower() if model else ""
    return any(rm in model_lower for rm in settings.llm.reasoning_models)


def get_llm(
    *,
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    max_tokens: Optional[int] = None,
    is_custom_llm: Optional[bool] = None,
    company_name: Optional[str] = None,
):
    """LLM 클라이언트 생성
    
    Args:
        api_key: API 키 (기본: 환경변수)
        base_url: API 기본 URL (기본: 환경변수)
        model: 모델명 (기본: 환경변수)
        max_tokens: 최대 토큰 (기본: 환경변수)
        is_custom_llm: 커스텀 LLM 사용 여부 (기본: 환경변수)
        company_name: 커스텀 LLM 회사명 (기본: 환경변수)
    
    Returns:
        ChatOpenAI 또는 CustomLLMClient 인스턴스
    """
    # LLM 캐싱 초기화
    _init_llm_cache()
    
    config = settings.llm
    
    # 매개변수 기본값 설정
    base_url = base_url or config.api_base
    api_key = api_key or config.api_key
    model = model or config.model
    max_tokens = max_tokens or config.max_tokens
    is_custom_llm = is_custom_llm if is_custom_llm is not None else config.is_custom
    company_name = company_name if company_name is not None else config.company_name

    if is_custom_llm:
        logging.debug("CustomLLM 사용: model=%s, company=%s", model, company_name)
        return CustomLLMClient(
            api_key=api_key,
            model=model,
            base_url=base_url,
            max_tokens=max_tokens,
            company_name=company_name,
        )

    # OpenAI 호환 클라이언트
    kwargs = {
        "model": model,
        "base_url": base_url,
        "api_key": api_key,
        "max_tokens": max_tokens,
        "temperature": config.temperature,
        "max_retries": 5,
    }

    # 추론 모델 특수 처리
    if _is_reasoning_model(model):
        kwargs["reasoning_effort"] = config.reasoning_effort
        logging.debug("추론 모델 사용: model=%s, effort=%s", model, kwargs["reasoning_effort"])

    return ChatOpenAI(**kwargs)

