"""용어 관리(Glossary) API 라우터

Neo4j 기반 비즈니스 용어집 관리 기능을 제공합니다.

스키마:
- Glossary: 용어집 (Business_Glossary, Technical_Glossary 등)
- Term: 용어 (Customer, Order 등)
- Domain: 도메인 (Sales, Finance 등)
- Owner: 소유자/검토자
- Tag: 태그

관계:
- (Glossary)-[:HAS_TERM]->(Term)
- (Term)-[:BELONGS_TO_DOMAIN]->(Domain)
- (Term)-[:OWNED_BY]->(Owner)
- (Term)-[:REVIEWED_BY]->(Owner)
- (Term)-[:HAS_TAG]->(Tag)
"""

import json
import logging
from typing import Any, Optional, List

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from config.settings import settings
from service import glossary_bulk_service, glossary_manage_service


router = APIRouter(prefix=f"{settings.api_prefix}/glossary")
logger = logging.getLogger(__name__)


def _require_non_empty_name(name: str, field_label: str) -> str:
    normalized = name.strip()
    if not normalized:
        raise HTTPException(400, f"{field_label} 이름을 입력해주세요.")
    return normalized


# =============================================================================
# 요청/응답 모델
# =============================================================================

class GlossaryCreate(BaseModel):
    """용어집 생성 요청"""
    name: str = Field(..., description="용어집 이름")
    description: str = Field("", description="용어집 설명")
    type: str = Field("Business", description="용어집 유형 (Business, Technical, DataQuality)")


class GlossaryUpdate(BaseModel):
    """용어집 수정 요청"""
    name: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None


class TermCreate(BaseModel):
    """용어 생성 요청"""
    name: str = Field(..., description="용어 이름")
    description: str = Field("", description="용어 설명")
    status: str = Field("Draft", description="상태 (Draft, Pending, Approved, Deprecated)")
    synonyms: List[str] = Field(default_factory=list, description="동의어 목록")
    relatedTerms: List[str] = Field(default_factory=list, description="관련 용어 ID 목록")
    domains: List[str] = Field(default_factory=list, description="도메인 목록")
    owners: List[str] = Field(default_factory=list, description="소유자 목록")
    reviewers: List[str] = Field(default_factory=list, description="검토자 목록")
    tags: List[str] = Field(default_factory=list, description="태그 목록")


class TermUpdate(BaseModel):
    """용어 수정 요청"""
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    synonyms: Optional[List[str]] = None
    relatedTerms: Optional[List[str]] = None
    domains: Optional[List[str]] = None
    owners: Optional[List[str]] = None
    reviewers: Optional[List[str]] = None
    tags: Optional[List[str]] = None


class DomainCreate(BaseModel):
    """도메인 생성 요청"""
    name: str = Field(..., description="도메인 이름")
    description: str = Field("", description="도메인 설명")


class OwnerCreate(BaseModel):
    """소유자 생성 요청"""
    name: str = Field(..., description="소유자 이름")
    email: str = Field("", description="이메일")
    role: str = Field("Owner", description="역할 (Owner, Reviewer)")


class TagCreate(BaseModel):
    """태그 생성 요청"""
    name: str = Field(..., description="태그 이름")
    color: str = Field("#3498db", description="태그 색상")


class BulkExtractRequest(BaseModel):
    """일괄 추출 요청"""
    session_id: str = Field(..., description="bulk-upload 세션 ID")
    confirmed_mapping: dict[str, Any] = Field(default_factory=dict, description="확정된 칼럼 매핑")
    confirmed_joins: List[dict[str, Any]] = Field(default_factory=list, description="확정된 조인 규칙")
    value_transforms: dict[str, Any] = Field(default_factory=dict, description="값 변환 규칙")
    use_ai_enrichment: bool = Field(False, description="AI 보강 사용 여부")


class BulkImportTerm(BaseModel):
    """일괄 가져오기용 용어"""
    name: str = Field(..., description="용어 이름")
    description: str = Field("", description="용어 설명")
    status: str = Field("Draft", description="상태")
    synonyms: List[str] = Field(default_factory=list, description="동의어 목록")
    domains: List[str] = Field(default_factory=list, description="도메인 목록")
    owners: List[str] = Field(default_factory=list, description="소유자 목록")
    reviewers: List[str] = Field(default_factory=list, description="검토자 목록")
    tags: List[str] = Field(default_factory=list, description="태그 목록")


class BulkImportRequest(BaseModel):
    """일괄 가져오기 요청"""
    terms: List[BulkImportTerm] = Field(default_factory=list, description="가져올 용어 목록")
    unmapped_columns_reminder: List[str] = Field(default_factory=list, description="매핑되지 않은 칼럼 안내")


def _extract_optional_api_key(request: Request) -> str:
    return (
        request.headers.get("OpenAI-Api-Key")
        or request.headers.get("Anthropic-Api-Key")
        or settings.llm.api_key
        or ""
    )


def _parse_stream_payload(payload: str) -> dict[str, Any]:
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        raise HTTPException(400, f"stream payload 파싱 실패: {exc}") from exc


# =============================================================================
# 용어집(Glossary) API
# =============================================================================

@router.get("/")
async def list_glossaries():
    """모든 용어집 목록 조회"""
    logger.info("[API] 용어집 목록 조회")
    try:
        return await glossary_manage_service.fetch_all_glossaries()
    except Exception as e:
        logger.error("[API] 용어집 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 목록 조회 실패: {e}")


@router.post("/")
async def create_glossary(body: GlossaryCreate):
    """새 용어집 생성"""
    logger.info("[API] 용어집 생성 | name=%s", body.name)
    try:
        return await glossary_manage_service.create_new_glossary(
            name=body.name,
            description=body.description,
            type_=body.type,
        )
    except Exception as e:
        logger.error("[API] 용어집 생성 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 생성 실패: {e}")


@router.get("/meta/domains")
async def list_domains():
    """모든 도메인 목록 조회"""
    try:
        return await glossary_manage_service.fetch_all_domains()
    except Exception as e:
        raise HTTPException(500, f"도메인 목록 조회 실패: {e}")


@router.get("/meta/owners")
async def list_owners():
    """모든 소유자/검토자 목록 조회"""
    try:
        return await glossary_manage_service.fetch_all_owners()
    except Exception as e:
        raise HTTPException(500, f"소유자 목록 조회 실패: {e}")


@router.get("/meta/tags")
async def list_tags():
    """모든 태그 목록 조회"""
    try:
        return await glossary_manage_service.fetch_all_tags()
    except Exception as e:
        raise HTTPException(500, f"태그 목록 조회 실패: {e}")


@router.post("/meta/domains")
async def create_domain(body: DomainCreate):
    """새 도메인 생성"""
    try:
        name = _require_non_empty_name(body.name, "도메인")
        return await glossary_manage_service.create_new_domain(name, body.description)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"도메인 생성 실패: {e}")


@router.post("/meta/owners")
async def create_owner(body: OwnerCreate):
    """새 소유자 생성"""
    try:
        name = _require_non_empty_name(body.name, "소유자")
        return await glossary_manage_service.create_new_owner(name, body.email, body.role)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"소유자 생성 실패: {e}")


@router.post("/meta/tags")
async def create_tag(body: TagCreate):
    """새 태그 생성"""
    try:
        name = _require_non_empty_name(body.name, "태그")
        return await glossary_manage_service.create_new_tag(name, body.color)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"태그 생성 실패: {e}")



@router.get("/{glossary_id}")
async def get_glossary(glossary_id: str):
    """특정 용어집 상세 조회"""
    logger.info("[API] 용어집 상세 조회 | id=%s", glossary_id)
    try:
        result = await glossary_manage_service.fetch_glossary_by_id(glossary_id)
        if result is None:
            raise HTTPException(404, "용어집을 찾을 수 없습니다.")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어집 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 조회 실패: {e}")


@router.put("/{glossary_id}")
async def update_glossary(glossary_id: str, body: GlossaryUpdate):
    """용어집 정보 수정"""
    logger.info("[API] 용어집 수정 | id=%s", glossary_id)
    try:
        result = await glossary_manage_service.update_glossary_info(
            glossary_id=glossary_id,
            name=body.name,
            description=body.description,
            type_=body.type,
        )
        if not result.get("updated"):
            raise HTTPException(404, "용어집을 찾을 수 없습니다.")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어집 수정 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 수정 실패: {e}")


@router.delete("/{glossary_id}")
async def delete_glossary(glossary_id: str):
    """용어집 삭제 (포함된 용어도 함께 삭제)"""
    logger.info("[API] 용어집 삭제 | id=%s", glossary_id)
    try:
        return await glossary_manage_service.delete_glossary_by_id(glossary_id)
    except Exception as e:
        logger.error("[API] 용어집 삭제 실패 | error=%s", e)
        raise HTTPException(500, f"용어집 삭제 실패: {e}")


# =============================================================================
# 용어(Term) API
# =============================================================================

@router.get("/{glossary_id}/terms")
async def list_terms(
    glossary_id: str,
    status: Optional[str] = None,
    search: Optional[str] = None,
):
    """용어집의 용어 목록 조회"""
    logger.info("[API] 용어 목록 조회 | glossary=%s", glossary_id)
    try:
        return await glossary_manage_service.fetch_glossary_terms(
            glossary_id=glossary_id,
            search=search,
        )
    except Exception as e:
        logger.error("[API] 용어 목록 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어 목록 조회 실패: {e}")


@router.post("/{glossary_id}/terms/bulk-upload")
async def bulk_upload_terms(
    glossary_id: str,
    request: Request,
    files: List[UploadFile] = File(...),
):
    """표준 용어 사전 파일 업로드 및 구조 분석"""
    logger.info("[API] 용어 일괄 업로드 | glossary=%s | file_count=%d", glossary_id, len(files))
    try:
        return await glossary_bulk_service.parse_uploaded_files(
            files=files,
            api_key=_extract_optional_api_key(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 일괄 업로드 실패 | error=%s", e)
        raise HTTPException(500, f"용어 일괄 업로드 실패: {e}")


@router.post("/{glossary_id}/terms/bulk-extract")
async def bulk_extract_terms(
    glossary_id: str,
    request: Request,
    body: BulkExtractRequest,
):
    """확정된 매핑으로 용어 후보 추출"""
    logger.info(
        "[API] 용어 일괄 추출 | glossary=%s | session=%s | ai=%s",
        glossary_id,
        body.session_id,
        body.use_ai_enrichment,
    )
    try:
        return await glossary_bulk_service.extract_term_candidates(
            session_id=body.session_id,
            confirmed_mapping=body.confirmed_mapping,
            confirmed_joins=body.confirmed_joins,
            value_transforms=body.value_transforms,
            use_ai_enrichment=body.use_ai_enrichment,
            api_key=_extract_optional_api_key(request),
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 일괄 추출 실패 | error=%s", e)
        raise HTTPException(500, f"용어 일괄 추출 실패: {e}")


@router.get("/{glossary_id}/terms/bulk-extract/stream")
async def bulk_extract_stream(
    glossary_id: str,
    request: Request,
    session_id: str,
    payload: str,
):
    """AI 보강 추출 진행률 스트리밍"""
    logger.info("[API] 용어 일괄 추출 스트리밍 | glossary=%s | session=%s", glossary_id, session_id)
    stream_payload = _parse_stream_payload(payload)
    return StreamingResponse(
        glossary_bulk_service.extract_with_streaming(
            session_id=session_id,
            confirmed_mapping=stream_payload.get("confirmed_mapping") or {},
            confirmed_joins=stream_payload.get("confirmed_joins") or [],
            value_transforms=stream_payload.get("value_transforms") or {},
            api_key=_extract_optional_api_key(request),
        ),
        media_type="text/event-stream",
    )


@router.post("/{glossary_id}/terms/bulk-import")
async def bulk_import_terms(glossary_id: str, body: BulkImportRequest):
    """추출된 용어 후보를 일괄 가져오기"""
    logger.info("[API] 용어 일괄 가져오기 | glossary=%s | terms=%d", glossary_id, len(body.terms))
    try:
        return await glossary_bulk_service.bulk_create_terms(
            glossary_id=glossary_id,
            terms=[term.model_dump() for term in body.terms],
            unmapped_columns_reminder=body.unmapped_columns_reminder,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 일괄 가져오기 실패 | error=%s", e)
        raise HTTPException(500, f"용어 일괄 가져오기 실패: {e}")


@router.delete("/{glossary_id}/terms/bulk-rollback/{batch_id}")
async def bulk_rollback(glossary_id: str, batch_id: str):
    """특정 batch_id로 생성된 용어 일괄 롤백"""
    logger.info("[API] 용어 일괄 롤백 | glossary=%s | batch=%s", glossary_id, batch_id)
    try:
        return await glossary_bulk_service.rollback_batch(glossary_id, batch_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 일괄 롤백 실패 | error=%s", e)
        raise HTTPException(500, f"용어 일괄 롤백 실패: {e}")


@router.post("/{glossary_id}/terms")
async def create_term(glossary_id: str, body: TermCreate):
    """새 용어 생성"""
    logger.info("[API] 용어 생성 | glossary=%s | name=%s", glossary_id, body.name)
    try:
        term_data = {
            "name": body.name,
            "description": body.description,
            "status": body.status,
            "synonyms": body.synonyms,
            "relatedTerms": body.relatedTerms,
            "domains": body.domains,
            "owners": body.owners,
            "reviewers": body.reviewers,
            "tags": body.tags,
        }
        return await glossary_manage_service.create_new_term(glossary_id, term_data)
    except Exception as e:
        logger.error("[API] 용어 생성 실패 | error=%s", e)
        raise HTTPException(500, f"용어 생성 실패: {e}")


@router.get("/{glossary_id}/terms/{term_id}")
async def get_term(glossary_id: str, term_id: str):
    """특정 용어 상세 조회"""
    logger.info("[API] 용어 상세 조회 | term=%s", term_id)
    try:
        result = await glossary_manage_service.fetch_term_by_id(glossary_id, term_id)
        if result is None:
            raise HTTPException(404, "용어를 찾을 수 없습니다.")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 조회 실패 | error=%s", e)
        raise HTTPException(500, f"용어 조회 실패: {e}")


@router.put("/{glossary_id}/terms/{term_id}")
async def update_term(glossary_id: str, term_id: str, body: TermUpdate):
    """용어 정보 수정"""
    logger.info("[API] 용어 수정 | term=%s", term_id)
    try:
        term_data = {
            "name": body.name,
            "description": body.description,
            "status": body.status,
            "synonyms": body.synonyms,
            "relatedTerms": body.relatedTerms,
            "domains": body.domains,
            "owners": body.owners,
            "reviewers": body.reviewers,
            "tags": body.tags,
        }
        result = await glossary_manage_service.update_term_info(glossary_id, term_id, term_data)
        if not result.get("updated"):
            raise HTTPException(404, "용어를 찾을 수 없습니다.")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error("[API] 용어 수정 실패 | error=%s", e)
        raise HTTPException(500, f"용어 수정 실패: {e}")


@router.delete("/{glossary_id}/terms/{term_id}")
async def delete_term(glossary_id: str, term_id: str):
    """용어 삭제"""
    logger.info("[API] 용어 삭제 | term=%s", term_id)
    try:
        return await glossary_manage_service.delete_term_by_id(glossary_id, term_id)
    except Exception as e:
        logger.error("[API] 용어 삭제 실패 | error=%s", e)
        raise HTTPException(500, f"용어 삭제 실패: {e}")


