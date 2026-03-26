"""용어 일괄 업로드 전용 서비스."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import math
import re
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any, AsyncGenerator, Iterable, Optional

import pandas as pd
from fastapi import HTTPException, UploadFile
from langchain_core.messages import HumanMessage, SystemMessage

from client.llm_client import get_llm
from client.neo4j_client import Neo4jClient
from config.settings import settings
try:
    from charset_normalizer import from_bytes as charset_normalizer_from_bytes
except ImportError:  # pragma: no cover - requirements sync 전 임시 폴백
    charset_normalizer_from_bytes = None

try:
    import chardet
except ImportError:  # pragma: no cover - fallback of fallback
    chardet = None


logger = logging.getLogger(__name__)

SESSION_TTL = timedelta(hours=1)
DEFAULT_IMPORT_CHUNK_SIZE = 500
VALID_TERM_STATUSES = ("Draft", "Pending", "Approved", "Deprecated")

HEADER_ALIASES = {
    "term_name": ["표준용어명", "용어명", "termname", "term", "name"],
    "term_description": ["용어설명", "설명", "description", "desc"],
    "term_status": ["상태코드", "상태", "status", "승인상태"],
    "term_synonyms": ["동의어", "동의어목록", "이음동의어목록", "synonyms", "synonym"],
    "term_domains": ["표준도메인명", "도메인명", "도메인", "domainname", "domain"],
    "word_name": ["표준단어명", "단어명", "wordname", "word"],
    "word_description": ["단어설명", "worddescription"],
    "word_synonyms": ["이음동의어목록", "동의어목록", "synonyms", "synonym"],
    "domain_name": ["도메인명", "표준도메인명", "domainname", "domain"],
    "domain_description": ["도메인설명", "description", "설명"],
}

ROLE_LABELS = ("term_source", "word_dictionary", "domain_dictionary", "code_dictionary", "unknown")
LIST_SPLIT_PATTERN = re.compile(r"[\n,;|/]+")


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _normalize_header_key(value: str) -> str:
    return re.sub(r"[\s_\-./()]+", "", value.strip().lower())


def _coerce_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _normalize_join_key(value: Any) -> str:
    text = _coerce_cell(value)
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().casefold()


def _dedupe_strings(values: Iterable[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = _coerce_cell(value)
        if not cleaned:
            continue
        key = cleaned.casefold()
        if key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def _split_multi_value(value: Any) -> list[str]:
    text = _coerce_cell(value)
    if not text:
        return []
    return _dedupe_strings(part.strip() for part in LIST_SPLIT_PATTERN.split(text))


def _clean_rows(rows: list[dict[str, Any]], headers: list[str]) -> list[dict[str, str]]:
    cleaned_rows: list[dict[str, str]] = []
    for row in rows:
        cleaned_rows.append({header: _coerce_cell(row.get(header, "")) for header in headers})
    return cleaned_rows


def _extract_json_object(text: str) -> dict[str, Any]:
    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?", "", raw).strip()
        raw = re.sub(r"```$", "", raw).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            return json.loads(raw[start:end + 1])
        raise


def _detect_csv_encoding(raw_bytes: bytes) -> str:
    if charset_normalizer_from_bytes is not None:
        result = charset_normalizer_from_bytes(raw_bytes).best()
        if result and result.encoding:
            return result.encoding
    if chardet is not None:
        detected = chardet.detect(raw_bytes)
        encoding = detected.get("encoding")
        if encoding:
            return encoding
    return "utf-8"


def _pick_header(headers: list[str], alias_key: str) -> Optional[str]:
    aliases = HEADER_ALIASES.get(alias_key, [])
    normalized_headers = {_normalize_header_key(header): header for header in headers}

    for alias in aliases:
        target = _normalize_header_key(alias)
        if target in normalized_headers:
            return normalized_headers[target]

    for header in headers:
        normalized = _normalize_header_key(header)
        if any(alias in normalized for alias in aliases):
            return header
    return None


def _value_source_column(value: Any) -> Optional[str]:
    if isinstance(value, str):
        cleaned = _coerce_cell(value)
        return cleaned or None
    if isinstance(value, dict):
        column = value.get("column")
        if isinstance(column, str) and column.strip():
            return column.strip()
    return None


def _merge_unmapped_columns(headers: list[str], mapped_values: dict[str, Any]) -> list[str]:
    mapped_columns = {
        column
        for column in (_value_source_column(value) for value in mapped_values.values())
        if column
    }
    return [header for header in headers if header not in mapped_columns]


def _normalize_status(value: Any, transforms: Optional[dict[str, str]] = None, default: str = "Draft") -> str:
    raw = _coerce_cell(value)
    if transforms:
        raw = transforms.get(raw, raw)

    if not raw:
        return default

    normalized = raw.strip().casefold()
    if normalized in ("draft", "초안"):
        return "Draft"
    if normalized in ("pending", "검토중", "검토 중"):
        return "Pending"
    if normalized in ("approved", "승인", "승인됨"):
        return "Approved"
    if normalized in ("deprecated", "폐기", "폐기됨"):
        return "Deprecated"

    for status in VALID_TERM_STATUSES:
        if normalized == status.casefold():
            return status
    return default


def _score_role(filename: str, headers: list[str], role: str) -> float:
    normalized_filename = filename.casefold()
    normalized_headers = {_normalize_header_key(header) for header in headers}

    score = 0.0
    if role == "term_source":
        if "glossary" in normalized_filename or "term" in normalized_filename:
            score += 0.4
        if "표준용어명" in normalized_headers:
            score += 0.5
        if _pick_header(headers, "term_name"):
            score += 0.2
        if _pick_header(headers, "term_description"):
            score += 0.1
    elif role == "word_dictionary":
        if "word" in normalized_filename:
            score += 0.4
        if "표준단어명" in normalized_headers:
            score += 0.5
        if _pick_header(headers, "word_name"):
            score += 0.2
        if _pick_header(headers, "word_synonyms"):
            score += 0.1
    elif role == "domain_dictionary":
        if "domain" in normalized_filename:
            score += 0.4
        if "도메인명" in normalized_headers or "도메인명" in "".join(normalized_headers):
            score += 0.4
        if _pick_header(headers, "domain_name"):
            score += 0.2
        if _pick_header(headers, "domain_description"):
            score += 0.1
    elif role == "code_dictionary":
        if "code" in normalized_filename:
            score += 0.4
        if any("코드" in header for header in headers):
            score += 0.2
    return min(score, 0.99)


def _heuristic_file_analysis(parsed_file: dict[str, Any]) -> dict[str, Any]:
    filename = parsed_file["filename"]
    headers = parsed_file["headers"]

    role_scores = {role: _score_role(filename, headers, role) for role in ROLE_LABELS}
    role = max(role_scores, key=role_scores.get)
    confidence = role_scores[role]
    if confidence < 0.25:
        role = "unknown"
        confidence = 0.2

    if role == "term_source":
        column_mapping = {
            "term_name": _pick_header(headers, "term_name"),
            "term_description": _pick_header(headers, "term_description"),
            "term_status": _pick_header(headers, "term_status"),
            "term_synonyms": _pick_header(headers, "term_synonyms"),
            "term_domains": _pick_header(headers, "term_domains"),
        }
    elif role == "word_dictionary":
        column_mapping = {
            "word_name": _pick_header(headers, "word_name"),
            "word_synonyms": _pick_header(headers, "word_synonyms"),
            "word_description": _pick_header(headers, "word_description"),
        }
    elif role == "domain_dictionary":
        column_mapping = {
            "domain_name": _pick_header(headers, "domain_name"),
            "domain_description": _pick_header(headers, "domain_description"),
        }
    else:
        column_mapping = {}

    return {
        "filename": filename,
        "role": role,
        "confidence": round(confidence, 2),
        "column_mapping": column_mapping,
        "unmapped_columns": _merge_unmapped_columns(headers, column_mapping),
    }


def _heuristic_join_rules(files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_name = {item["filename"]: item for item in files}
    term_source = next((item for item in files if item["role"] == "term_source"), None)
    if term_source is None:
        return []

    join_rules: list[dict[str, Any]] = []
    term_domains = term_source["column_mapping"].get("term_domains")
    term_name = term_source["column_mapping"].get("term_name")

    for file_info in files:
        if file_info["filename"] == term_source["filename"]:
            continue
        if file_info["role"] == "domain_dictionary":
            domain_name = file_info["column_mapping"].get("domain_name")
            if term_domains and domain_name:
                join_rules.append({
                    "left_file": term_source["filename"],
                    "left_column": term_domains,
                    "right_file": file_info["filename"],
                    "right_column": domain_name,
                    "join_type": "exact_match",
                    "purpose": "용어에 도메인 상세 정보를 보충",
                    "confidence": 0.95,
                })
        if file_info["role"] == "word_dictionary":
            word_name = file_info["column_mapping"].get("word_name")
            if term_name and word_name:
                join_rules.append({
                    "left_file": term_source["filename"],
                    "left_column": term_name,
                    "right_file": file_info["filename"],
                    "right_column": word_name,
                    "join_type": "exact_match",
                    "purpose": "용어에 동의어 및 보조 설명을 보충",
                    "confidence": 0.9,
                })

    return join_rules


def _normalize_analysis_payload(payload: dict[str, Any], fallback_files: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_files: list[dict[str, Any]] = []
    fallback_by_name = {item["filename"]: item for item in fallback_files}

    for file_payload in payload.get("files", []):
        filename = _coerce_cell(file_payload.get("filename"))
        fallback = fallback_by_name.get(filename)
        if not filename or fallback is None:
            continue

        raw_mapping = file_payload.get("column_mapping") or {}
        column_mapping = {
            key: (_coerce_cell(value) or None)
            for key, value in raw_mapping.items()
        }

        normalized_files.append({
            "filename": filename,
            "role": file_payload.get("role") if file_payload.get("role") in ROLE_LABELS else fallback["role"],
            "confidence": round(float(file_payload.get("confidence", fallback["confidence"])), 2),
            "column_mapping": column_mapping or fallback["column_mapping"],
            "unmapped_columns": _dedupe_strings(file_payload.get("unmapped_columns") or fallback["unmapped_columns"]),
        })

    if not normalized_files:
        normalized_files = fallback_files

    term_source_file = _coerce_cell(payload.get("term_source_file"))
    no_term_source = bool(payload.get("no_term_source"))
    if not term_source_file:
        detected_term_source = next((item["filename"] for item in normalized_files if item["role"] == "term_source"), "")
        term_source_file = detected_term_source
        no_term_source = not bool(term_source_file)

    join_rules: list[dict[str, Any]] = []
    for join_rule in payload.get("join_rules", []):
        left_file = _coerce_cell(join_rule.get("left_file"))
        right_file = _coerce_cell(join_rule.get("right_file"))
        left_column = _coerce_cell(join_rule.get("left_column"))
        right_column = _coerce_cell(join_rule.get("right_column"))
        if not (left_file and right_file and left_column and right_column):
            continue
        join_rules.append({
            "left_file": left_file,
            "left_column": left_column,
            "right_file": right_file,
            "right_column": right_column,
            "join_type": _coerce_cell(join_rule.get("join_type")) or "exact_match",
            "purpose": _coerce_cell(join_rule.get("purpose")) or "보조 데이터 조인",
            "confidence": round(float(join_rule.get("confidence", 0.8)), 2),
        })

    return {
        "files": normalized_files,
        "join_rules": join_rules,
        "term_source_file": term_source_file or None,
        "no_term_source": no_term_source,
    }


class SessionStore:
    def save(self, session_id: str, payload: dict[str, Any]) -> None:
        raise NotImplementedError

    def get(self, session_id: str) -> Optional[dict[str, Any]]:
        raise NotImplementedError

    def update(self, session_id: str, patch: dict[str, Any]) -> None:
        raise NotImplementedError

    def delete(self, session_id: str) -> None:
        raise NotImplementedError

    def cleanup_expired(self) -> int:
        raise NotImplementedError


class InMemorySessionStore(SessionStore):
    def __init__(self) -> None:
        self._store: dict[str, dict[str, Any]] = {}

    def save(self, session_id: str, payload: dict[str, Any]) -> None:
        self._store[session_id] = payload

    def get(self, session_id: str) -> Optional[dict[str, Any]]:
        return self._store.get(session_id)

    def update(self, session_id: str, patch: dict[str, Any]) -> None:
        if session_id not in self._store:
            raise KeyError(session_id)
        self._store[session_id].update(patch)

    def delete(self, session_id: str) -> None:
        self._store.pop(session_id, None)

    def cleanup_expired(self) -> int:
        expired_ids = []
        now = _utc_now()
        for session_id, payload in self._store.items():
            created_at = payload.get("created_at")
            if not isinstance(created_at, datetime):
                expired_ids.append(session_id)
                continue
            if now - created_at > SESSION_TTL:
                expired_ids.append(session_id)
        for session_id in expired_ids:
            self._store.pop(session_id, None)
        return len(expired_ids)


_session_store: SessionStore = InMemorySessionStore()


def cleanup_expired_sessions() -> int:
    return _session_store.cleanup_expired()


def _get_session_or_404(session_id: str) -> dict[str, Any]:
    cleanup_expired_sessions()
    session = _session_store.get(session_id)
    if session is None:
        raise HTTPException(404, "일괄 업로드 세션을 찾을 수 없거나 만료되었습니다.")
    return session


def _session_public_files(parsed_files: list[dict[str, Any]]) -> list[dict[str, Any]]:
    public_files: list[dict[str, Any]] = []
    for item in parsed_files:
        public_files.append({
            "filename": item["filename"],
            "headers": item["headers"],
            "sample_rows": item["sample_rows"],
            "total_rows": item["total_rows"],
            "detected_encoding": item.get("detected_encoding"),
            "parse_status": item.get("parse_status", "success"),
        })
    return public_files


def _build_structure_prompt(parsed_files: list[dict[str, Any]], heuristic_hint: dict[str, Any]) -> str:
    files_section = []
    for index, parsed_file in enumerate(parsed_files, start=1):
        files_section.append(
            "\n".join([
                f"### 파일 {index}: {parsed_file['filename']}",
                f"헤더: {parsed_file['headers']}",
                f"샘플 (상위 5행): {json.dumps(parsed_file['sample_rows'], ensure_ascii=False)}",
            ])
        )

    response_schema = {
        "files": [
            {
                "filename": "...",
                "role": "term_source | word_dictionary | domain_dictionary | code_dictionary | unknown",
                "confidence": 0.95,
                "column_mapping": {
                    "term_name": None,
                    "term_description": None,
                    "term_status": None,
                    "term_synonyms": None,
                    "term_domains": None,
                },
                "unmapped_columns": [],
            }
        ],
        "join_rules": [
            {
                "left_file": "...",
                "left_column": "...",
                "right_file": "...",
                "right_column": "...",
                "join_type": "exact_match",
                "purpose": "...",
                "confidence": 0.95,
            }
        ],
        "term_source_file": "...",
        "no_term_source": False,
    }

    return "\n".join([
        "당신은 데이터 구조 분석 전문가입니다.",
        "",
        "아래는 사용자가 업로드한 파일들의 칼럼 헤더와 샘플 데이터입니다.",
        "이 파일들을 분석하여 용어(Term)를 추출하기 위한 매핑 정보를 JSON으로 반환하세요.",
        "",
        "## Term 노드 스키마",
        "- name (string, 필수): 용어의 이름",
        "- description (string): 용어의 설명",
        "- status (string): Draft / Pending / Approved / Deprecated",
        "- synonyms (list[string]): 동의어 목록",
        "- domains (list[string]): 도메인 분류명 목록",
        "",
        "## 업로드된 파일들",
        *files_section,
        "",
        "## 휴리스틱 힌트",
        json.dumps(heuristic_hint, ensure_ascii=False, indent=2),
        "",
        "## 판단 기준",
        "1. 각 파일의 역할을 term_source / word_dictionary / domain_dictionary / code_dictionary / unknown 중 하나로 지정하세요.",
        "2. term_source 파일의 name, description, status, synonyms, domains 매핑을 최대한 채우세요.",
        "3. 파일 간 조인 규칙이 보이면 join_rules에 넣으세요.",
        "4. 매핑되지 않는 칼럼은 unmapped_columns에 넣으세요.",
        "5. JSON만 반환하고, 설명 텍스트는 넣지 마세요.",
        "",
        "## 응답 형식",
        json.dumps(response_schema, ensure_ascii=False, indent=2),
    ])


async def _invoke_structure_llm(parsed_files: list[dict[str, Any]], heuristic_hint: dict[str, Any], api_key: str) -> dict[str, Any]:
    prompt = _build_structure_prompt(parsed_files, heuristic_hint)
    llm = get_llm(api_key=api_key)
    messages = [
        SystemMessage(content="JSON만 반환하는 데이터 구조 분석 전문가입니다."),
        HumanMessage(content=prompt),
    ]

    response = await asyncio.to_thread(llm.invoke, messages)
    raw_content = response.content if hasattr(response, "content") else str(response)
    return _extract_json_object(raw_content)


async def analyze_file_structure(parsed_files: list[dict[str, Any]], api_key: str = "") -> dict[str, Any]:
    heuristic_files = [_heuristic_file_analysis(parsed_file) for parsed_file in parsed_files]
    heuristic_result = {
        "files": heuristic_files,
        "join_rules": _heuristic_join_rules(heuristic_files),
        "term_source_file": next((item["filename"] for item in heuristic_files if item["role"] == "term_source"), None),
        "no_term_source": not any(item["role"] == "term_source" for item in heuristic_files),
    }

    effective_api_key = api_key or settings.llm.api_key
    if not effective_api_key:
        return heuristic_result

    try:
        llm_payload = await _invoke_structure_llm(parsed_files, heuristic_result, effective_api_key)
        normalized = _normalize_analysis_payload(llm_payload, heuristic_files)
        if not normalized["join_rules"]:
            normalized["join_rules"] = heuristic_result["join_rules"]
        if not normalized["term_source_file"] and heuristic_result["term_source_file"]:
            normalized["term_source_file"] = heuristic_result["term_source_file"]
            normalized["no_term_source"] = False
        return normalized
    except Exception as exc:
        logger.warning("[GLOSSARY:BULK] 구조 분석 LLM 실패, 휴리스틱으로 대체 | error=%s", exc)
        return heuristic_result


async def parse_uploaded_files(files: list[UploadFile], api_key: str = "") -> dict[str, Any]:
    cleanup_expired_sessions()
    if not files:
        raise HTTPException(400, "업로드할 파일을 선택해주세요.")

    parsed_files: list[dict[str, Any]] = []
    for upload in files:
        filename = _coerce_cell(upload.filename)
        if not filename:
            raise HTTPException(400, "파일 이름이 비어 있습니다.")

        raw_bytes = await upload.read()
        lowered = filename.casefold()
        try:
            if lowered.endswith(".csv"):
                detected_encoding = _detect_csv_encoding(raw_bytes)
                dataframe = pd.read_csv(
                    io.BytesIO(raw_bytes),
                    encoding=detected_encoding,
                    dtype=str,
                    keep_default_na=False,
                )
            elif lowered.endswith(".xlsx") or lowered.endswith(".xls"):
                detected_encoding = None
                dataframe = pd.read_excel(
                    io.BytesIO(raw_bytes),
                    dtype=str,
                    keep_default_na=False,
                )
            else:
                raise HTTPException(400, f"지원하지 않는 파일 형식입니다: {filename}")
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(400, f"파일 파싱 실패: {filename} ({exc})") from exc

        dataframe = dataframe.fillna("")
        headers = [_coerce_cell(column) for column in dataframe.columns.tolist()]
        dataframe.columns = headers
        rows = _clean_rows(dataframe.to_dict(orient="records"), headers)

        parsed_files.append({
            "filename": filename,
            "headers": headers,
            "sample_rows": [[row.get(header, "") for header in headers] for row in rows[:5]],
            "total_rows": len(rows),
            "detected_encoding": detected_encoding,
            "parse_status": "success",
            "all_rows": rows,
        })

    analysis = await analyze_file_structure(parsed_files, api_key=api_key)
    public_files = _session_public_files(parsed_files)

    files_by_name = {item["filename"]: item for item in public_files}
    for analysis_file in analysis["files"]:
        if analysis_file["filename"] not in files_by_name:
            continue
        files_by_name[analysis_file["filename"]].update({
            "role": analysis_file["role"],
            "confidence": analysis_file["confidence"],
            "column_mapping": analysis_file["column_mapping"],
            "unmapped_columns": analysis_file["unmapped_columns"],
        })

    session_id = str(uuid.uuid4())
    _session_store.save(session_id, {
        "created_at": _utc_now(),
        "files": parsed_files,
        "analysis": analysis,
        "last_extract_request": None,
        "last_extract_result": None,
        "stream_partial_result": None,
    })

    return {
        "session_id": session_id,
        "files": list(files_by_name.values()),
        "join_rules": analysis["join_rules"],
        "term_source_file": analysis["term_source_file"],
        "no_term_source": analysis["no_term_source"],
    }


def _resolve_mapping_spec(value: Any) -> Optional[dict[str, Any]]:
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = _coerce_cell(value)
        if not cleaned:
            return None
        return {"source": "source", "column": cleaned}
    if isinstance(value, dict):
        source = _coerce_cell(value.get("source")) or "joined"
        column = _coerce_cell(value.get("column"))
        file_name = _coerce_cell(value.get("file")) or None
        if not column:
            return None
        join_rule_index = value.get("join_rule_index")
        try:
            join_rule_index = int(join_rule_index) if join_rule_index is not None else None
        except (TypeError, ValueError):
            join_rule_index = None
        return {
            "source": source,
            "column": column,
            "file": file_name,
            "join_rule_index": join_rule_index,
        }
    return None


def _is_empty_mapping_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, dict):
        return not _coerce_cell(value.get("column"))
    return False


def _build_file_lookup(parsed_files: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {item["filename"]: item for item in parsed_files}


def _match_join_rows(
    source_row: dict[str, str],
    source_file_name: str,
    join_rule: dict[str, Any],
    file_lookup: dict[str, dict[str, Any]],
) -> list[dict[str, str]]:
    left_file = join_rule["left_file"]
    right_file = join_rule["right_file"]
    left_column = join_rule["left_column"]
    right_column = join_rule["right_column"]

    if source_file_name == left_file:
        target_file_name = right_file
        source_column = left_column
        target_column = right_column
    elif source_file_name == right_file:
        target_file_name = left_file
        source_column = right_column
        target_column = left_column
    else:
        return []

    source_key = _normalize_join_key(source_row.get(source_column))
    if not source_key:
        return []

    target_file = file_lookup.get(target_file_name)
    if target_file is None:
        return []

    matched_rows: list[dict[str, str]] = []
    for candidate in target_file["all_rows"]:
        if _normalize_join_key(candidate.get(target_column)) == source_key:
            matched_rows.append(candidate)
    return matched_rows


def _resolve_field_values(
    spec_value: Any,
    field_name: str,
    source_row: dict[str, str],
    source_file_name: str,
    confirmed_joins: list[dict[str, Any]],
    file_lookup: dict[str, dict[str, Any]],
) -> list[str]:
    spec = _resolve_mapping_spec(spec_value)
    if spec is None:
        return []

    if spec["source"] == "source" or not spec.get("file"):
        value = source_row.get(spec["column"], "")
        if field_name in ("synonyms", "domains"):
            return _split_multi_value(value)
        return [_coerce_cell(value)] if _coerce_cell(value) else []

    join_candidates = []
    if spec.get("join_rule_index") is not None:
        rule_index = spec["join_rule_index"]
        if 0 <= rule_index < len(confirmed_joins):
            join_candidates = [confirmed_joins[rule_index]]
    else:
        join_candidates = [
            rule for rule in confirmed_joins
            if spec.get("file") in (rule.get("left_file"), rule.get("right_file"))
        ]

    collected: list[str] = []
    for join_rule in join_candidates:
        for matched_row in _match_join_rows(source_row, source_file_name, join_rule, file_lookup):
            if field_name in ("synonyms", "domains"):
                collected.extend(_split_multi_value(matched_row.get(spec["column"], "")))
            else:
                value = _coerce_cell(matched_row.get(spec["column"], ""))
                if value:
                    collected.append(value)
    return _dedupe_strings(collected)


def _calculate_field_statistics(term_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    status_distribution = Counter()
    filled_counts = {
        "name": 0,
        "description": 0,
        "status": 0,
        "synonyms": 0,
        "domains": 0,
    }

    for candidate in term_candidates:
        if _coerce_cell(candidate.get("name")):
            filled_counts["name"] += 1
        if _coerce_cell(candidate.get("description")):
            filled_counts["description"] += 1
        if _coerce_cell(candidate.get("status")):
            filled_counts["status"] += 1
            status_distribution[_coerce_cell(candidate.get("status"))] += 1
        if candidate.get("synonyms"):
            filled_counts["synonyms"] += 1
        if candidate.get("domains"):
            filled_counts["domains"] += 1

    total = len(term_candidates)
    return {
        "name": {"filled": filled_counts["name"], "empty": total - filled_counts["name"]},
        "description": {"filled": filled_counts["description"], "empty": total - filled_counts["description"]},
        "status": {
            "filled": filled_counts["status"],
            "empty": total - filled_counts["status"],
            "distribution": dict(status_distribution),
        },
        "synonyms": {"filled": filled_counts["synonyms"], "empty": total - filled_counts["synonyms"]},
        "domains": {"filled": filled_counts["domains"], "empty": total - filled_counts["domains"]},
    }


def _aggregate_unmapped_columns(analysis: dict[str, Any]) -> list[str]:
    aggregated: list[str] = []
    for file_info in analysis.get("files", []):
        aggregated.extend(file_info.get("unmapped_columns") or [])
    return _dedupe_strings(aggregated)


def _base_term_candidate(
    row_index: int,
    source_row: dict[str, str],
    source_file_name: str,
    confirmed_mapping: dict[str, Any],
    confirmed_joins: list[dict[str, Any]],
    value_transforms: dict[str, Any],
    file_lookup: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    name_values = _resolve_field_values(
        confirmed_mapping.get("term_name_column"),
        "name",
        source_row,
        source_file_name,
        confirmed_joins,
        file_lookup,
    )
    description_values = _resolve_field_values(
        confirmed_mapping.get("term_description_column"),
        "description",
        source_row,
        source_file_name,
        confirmed_joins,
        file_lookup,
    )
    status_values = _resolve_field_values(
        confirmed_mapping.get("term_status_column"),
        "status",
        source_row,
        source_file_name,
        confirmed_joins,
        file_lookup,
    )
    synonyms = _resolve_field_values(
        confirmed_mapping.get("term_synonyms_column"),
        "synonyms",
        source_row,
        source_file_name,
        confirmed_joins,
        file_lookup,
    )
    domains = _resolve_field_values(
        confirmed_mapping.get("term_domains_column"),
        "domains",
        source_row,
        source_file_name,
        confirmed_joins,
        file_lookup,
    )

    fixed_status = _coerce_cell(confirmed_mapping.get("term_status_fixed"))
    status_transforms = None
    if not fixed_status and isinstance(value_transforms, dict):
        status_transforms = value_transforms.get("term_status")
    resolved_status = fixed_status or (status_values[0] if status_values else "")

    return {
        "row_index": row_index,
        "name": name_values[0] if name_values else "",
        "description": description_values[0] if description_values else "",
        "status": _normalize_status(resolved_status, status_transforms),
        "synonyms": _dedupe_strings(synonyms),
        "domains": _dedupe_strings(domains),
        "tags": [],
    }


def _join_statistics_template(confirmed_joins: list[dict[str, Any]]) -> list[dict[str, Any]]:
    templates: list[dict[str, Any]] = []
    for join_rule in confirmed_joins:
        templates.append({
            "rule": f"{join_rule['left_file']}[{join_rule['left_column']}] = {join_rule['right_file']}[{join_rule['right_column']}]",
            "matched": 0,
            "unmatched": 0,
            "total": 0,
        })
    return templates


def _apply_join_statistics(
    source_row: dict[str, str],
    source_file_name: str,
    confirmed_joins: list[dict[str, Any]],
    file_lookup: dict[str, dict[str, Any]],
    join_statistics: list[dict[str, Any]],
) -> None:
    for index, join_rule in enumerate(confirmed_joins):
        if source_file_name not in (join_rule["left_file"], join_rule["right_file"]):
            continue
        matches = _match_join_rows(source_row, source_file_name, join_rule, file_lookup)
        join_statistics[index]["total"] += 1
        if matches:
            join_statistics[index]["matched"] += 1
        else:
            join_statistics[index]["unmatched"] += 1


async def _invoke_row_enrichment_llm(
    candidate: dict[str, Any],
    current_row: dict[str, str],
    joined_rows: list[dict[str, str]],
    api_key: str,
) -> dict[str, Any]:
    prompt = "\n".join([
        "아래 데이터를 바탕으로 용어(Term) 항목을 JSON으로 생성하세요.",
        "",
        "## 현재 처리할 행",
        json.dumps(current_row, ensure_ascii=False, indent=2),
        "",
        "## 조인된 메타데이터",
        json.dumps(joined_rows, ensure_ascii=False, indent=2),
        "",
        "## 현재 추출 결과 초안",
        json.dumps(candidate, ensure_ascii=False, indent=2),
        "",
        "## 출력 형식 (JSON만 반환)",
        json.dumps({
            "name": "용어 이름",
            "description": "용어 설명",
            "status": "Draft",
            "synonyms": [],
            "domains": [],
            "skip": False,
            "skip_reason": None,
        }, ensure_ascii=False, indent=2),
    ])

    llm = get_llm(api_key=api_key)
    messages = [
        SystemMessage(content="JSON만 반환하는 용어 정제 전문가입니다."),
        HumanMessage(content=prompt),
    ]
    response = await asyncio.to_thread(llm.invoke, messages)
    raw_content = response.content if hasattr(response, "content") else str(response)
    return _extract_json_object(raw_content)


async def _enrich_candidate_if_needed(
    candidate: dict[str, Any],
    source_row: dict[str, str],
    source_file_name: str,
    confirmed_joins: list[dict[str, Any]],
    file_lookup: dict[str, dict[str, Any]],
    api_key: str,
) -> Optional[dict[str, Any]]:
    joined_rows: list[dict[str, str]] = []
    for join_rule in confirmed_joins:
        joined_rows.extend(_match_join_rows(source_row, source_file_name, join_rule, file_lookup))

    if not api_key:
        if not candidate["description"]:
            for joined_row in joined_rows:
                description = joined_row.get("단어설명") or joined_row.get("도메인 설명") or joined_row.get("description")
                description = _coerce_cell(description)
                if description:
                    candidate["description"] = description
                    break
        if not candidate["synonyms"]:
            for joined_row in joined_rows:
                candidate["synonyms"] = _split_multi_value(
                    joined_row.get("이음동의어 목록") or joined_row.get("동의어 목록") or joined_row.get("synonyms")
                )
                if candidate["synonyms"]:
                    break
        return candidate

    try:
        enriched = await _invoke_row_enrichment_llm(candidate, source_row, joined_rows, api_key)
        if enriched.get("skip"):
            return None
        return {
            "row_index": candidate["row_index"],
            "name": _coerce_cell(enriched.get("name")) or candidate["name"],
            "description": _coerce_cell(enriched.get("description")) or candidate["description"],
            "status": _normalize_status(enriched.get("status") or candidate["status"]),
            "synonyms": _dedupe_strings(enriched.get("synonyms") or candidate["synonyms"]),
            "domains": _dedupe_strings(enriched.get("domains") or candidate["domains"]),
            "tags": candidate.get("tags", []),
        }
    except Exception as exc:
        logger.warning("[GLOSSARY:BULK] 행별 보강 실패, 기본 추출값 사용 | row=%s | error=%s", candidate["row_index"], exc)
        return candidate


def _extract_term_candidates_sync(
    session: dict[str, Any],
    confirmed_mapping: dict[str, Any],
    confirmed_joins: list[dict[str, Any]],
    value_transforms: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parsed_files = session["files"]
    file_lookup = _build_file_lookup(parsed_files)
    source_file_name = _coerce_cell(
        confirmed_mapping.get("term_source_file")
        or session.get("analysis", {}).get("term_source_file")
        or ""
    )
    if not source_file_name:
        raise HTTPException(400, "용어 소스 파일이 확정되지 않았습니다.")

    source_file = file_lookup.get(source_file_name)
    if source_file is None:
        raise HTTPException(400, f"용어 소스 파일을 찾을 수 없습니다: {source_file_name}")

    join_statistics = _join_statistics_template(confirmed_joins)
    term_candidates: list[dict[str, Any]] = []

    for row_index, source_row in enumerate(source_file["all_rows"]):
        _apply_join_statistics(source_row, source_file_name, confirmed_joins, file_lookup, join_statistics)
        candidate = _base_term_candidate(
            row_index=row_index,
            source_row=source_row,
            source_file_name=source_file_name,
            confirmed_mapping=confirmed_mapping,
            confirmed_joins=confirmed_joins,
            value_transforms=value_transforms,
            file_lookup=file_lookup,
        )
        term_candidates.append(candidate)

    return term_candidates, join_statistics


def _build_extract_response(
    term_candidates: list[dict[str, Any]],
    join_statistics: list[dict[str, Any]],
    analysis: dict[str, Any],
    enriched: bool,
) -> dict[str, Any]:
    return {
        "term_candidates": term_candidates,
        "total": len(term_candidates),
        "enriched": enriched,
        "join_statistics": join_statistics,
        "field_statistics": _calculate_field_statistics(term_candidates),
        "unmapped_columns": _aggregate_unmapped_columns(analysis),
    }


def _request_signature(
    confirmed_mapping: dict[str, Any],
    confirmed_joins: list[dict[str, Any]],
    value_transforms: dict[str, Any],
    use_ai_enrichment: bool,
) -> str:
    return json.dumps({
        "confirmed_mapping": confirmed_mapping,
        "confirmed_joins": confirmed_joins,
        "value_transforms": value_transforms,
        "use_ai_enrichment": use_ai_enrichment,
    }, ensure_ascii=False, sort_keys=True)


async def extract_term_candidates(
    session_id: str,
    confirmed_mapping: dict[str, Any],
    confirmed_joins: list[dict[str, Any]],
    value_transforms: dict[str, Any],
    use_ai_enrichment: bool,
    api_key: str = "",
) -> dict[str, Any]:
    session = _get_session_or_404(session_id)
    current_request = {
        "confirmed_mapping": confirmed_mapping,
        "confirmed_joins": confirmed_joins,
        "value_transforms": value_transforms,
        "use_ai_enrichment": use_ai_enrichment,
    }
    current_signature = _request_signature(**current_request)
    saved_request = session.get("last_extract_request") or {}
    saved_signature = _request_signature(
        confirmed_mapping=saved_request.get("confirmed_mapping") or {},
        confirmed_joins=saved_request.get("confirmed_joins") or [],
        value_transforms=saved_request.get("value_transforms") or {},
        use_ai_enrichment=bool(saved_request.get("use_ai_enrichment")),
    ) if saved_request else ""

    if current_signature == saved_signature:
        if use_ai_enrichment and session.get("stream_partial_result"):
            return session["stream_partial_result"]
        if session.get("last_extract_result"):
            return session["last_extract_result"]

    session["last_extract_request"] = current_request

    term_candidates, join_statistics = _extract_term_candidates_sync(
        session,
        confirmed_mapping,
        confirmed_joins,
        value_transforms,
    )

    if use_ai_enrichment:
        file_lookup = _build_file_lookup(session["files"])
        source_file_name = _coerce_cell(confirmed_mapping.get("term_source_file") or session["analysis"]["term_source_file"])
        source_rows = file_lookup[source_file_name]["all_rows"]
        enriched_candidates: list[dict[str, Any]] = []
        effective_api_key = api_key or settings.llm.api_key
        for candidate in term_candidates:
            enriched = await _enrich_candidate_if_needed(
                candidate=candidate,
                source_row=source_rows[candidate["row_index"]],
                source_file_name=source_file_name,
                confirmed_joins=confirmed_joins,
                file_lookup=file_lookup,
                api_key=effective_api_key,
            )
            if enriched is not None:
                enriched_candidates.append(enriched)
        term_candidates = enriched_candidates

    response = _build_extract_response(
        term_candidates=term_candidates,
        join_statistics=join_statistics,
        analysis=session["analysis"],
        enriched=use_ai_enrichment,
    )
    _session_store.update(session_id, {"last_extract_result": response})
    return response


def _format_sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def extract_with_streaming(
    session_id: str,
    confirmed_mapping: Optional[dict[str, Any]] = None,
    confirmed_joins: Optional[list[dict[str, Any]]] = None,
    value_transforms: Optional[dict[str, Any]] = None,
    api_key: str = "",
) -> AsyncGenerator[str, None]:
    session = _get_session_or_404(session_id)
    last_request = session.get("last_extract_request") or {}

    effective_mapping = confirmed_mapping or last_request.get("confirmed_mapping") or {}
    effective_joins = confirmed_joins or last_request.get("confirmed_joins") or []
    effective_transforms = value_transforms or last_request.get("value_transforms") or {}
    effective_api_key = api_key or settings.llm.api_key

    session["last_extract_request"] = {
        "confirmed_mapping": effective_mapping,
        "confirmed_joins": effective_joins,
        "value_transforms": effective_transforms,
        "use_ai_enrichment": True,
    }

    base_candidates, join_statistics = _extract_term_candidates_sync(
        session,
        effective_mapping,
        effective_joins,
        effective_transforms,
    )

    file_lookup = _build_file_lookup(session["files"])
    source_file_name = _coerce_cell(effective_mapping.get("term_source_file") or session["analysis"]["term_source_file"])
    source_rows = file_lookup[source_file_name]["all_rows"]
    total = len(base_candidates)
    enriched_candidates: list[dict[str, Any]] = []
    started_at = _utc_now()

    try:
        for index, candidate in enumerate(base_candidates, start=1):
            enriched = await _enrich_candidate_if_needed(
                candidate=candidate,
                source_row=source_rows[candidate["row_index"]],
                source_file_name=source_file_name,
                confirmed_joins=effective_joins,
                file_lookup=file_lookup,
                api_key=effective_api_key,
            )
            if enriched is not None:
                enriched_candidates.append(enriched)

            elapsed = max((_utc_now() - started_at).total_seconds(), 0.001)
            avg = elapsed / index
            remaining = max(total - index, 0)
            yield _format_sse("progress", {
                "processed": index,
                "total": total,
                "estimated_remaining_seconds": math.ceil(avg * remaining),
            })

        result = _build_extract_response(
            term_candidates=enriched_candidates,
            join_statistics=join_statistics,
            analysis=session["analysis"],
            enriched=True,
        )
        _session_store.update(session_id, {
            "last_extract_result": result,
            "stream_partial_result": result,
        })
        yield _format_sse("complete", result)
    except asyncio.CancelledError:
        partial_result = _build_extract_response(
            term_candidates=enriched_candidates,
            join_statistics=join_statistics,
            analysis=session["analysis"],
            enriched=True,
        )
        _session_store.update(session_id, {"stream_partial_result": partial_result})
        raise


def _sanitize_import_term(term: dict[str, Any], batch_id: str) -> dict[str, Any]:
    return {
        "name": _coerce_cell(term.get("name")),
        "description": _coerce_cell(term.get("description")),
        "status": _normalize_status(term.get("status")),
        "synonyms": _dedupe_strings(term.get("synonyms") or []),
        "domains": _dedupe_strings(term.get("domains") or []),
        "owners": _dedupe_strings(term.get("owners") or []),
        "reviewers": _dedupe_strings(term.get("reviewers") or []),
        "tags": _dedupe_strings(term.get("tags") or []),
        "batch_id": batch_id,
    }


async def _fetch_existing_term_names(glossary_id: str, names: list[str]) -> set[str]:
    if not names:
        return set()
    query = {
        "query": """
            MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
            WHERE elementId(__cy_g__) = $glossary_id
              AND __cy_t__.name IN $names
            RETURN __cy_t__.name AS name
        """,
        "parameters": {"glossary_id": glossary_id, "names": names},
    }
    async with Neo4jClient() as client:
        result = await client.execute_queries([query])
    return {record["name"] for record in (result[0] if result else []) if record.get("name")}


async def _fetch_existing_domain_names(names: list[str]) -> set[str]:
    if not names:
        return set()
    query = {
        "query": """
            MATCH (__cy_d__:Domain)
            WHERE __cy_d__.name IN $names
            RETURN __cy_d__.name AS name
        """,
        "parameters": {"names": names},
    }
    async with Neo4jClient() as client:
        result = await client.execute_queries([query])
    return {record["name"] for record in (result[0] if result else []) if record.get("name")}


async def _insert_term_chunk(glossary_id: str, batch_id: str, terms: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not terms:
        return []

    query = {
        "query": """
            MATCH (__cy_g__:Glossary)
            WHERE elementId(__cy_g__) = $glossary_id
            UNWIND $terms AS term
            CREATE (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term {
                name: term.name,
                description: coalesce(term.description, ''),
                status: coalesce(term.status, 'Draft'),
                synonyms: coalesce(term.synonyms, []),
                batch_id: $batch_id,
                created_at: $timestamp,
                updated_at: $timestamp
            })
            FOREACH (domain_name IN coalesce(term.domains, []) |
                MERGE (__cy_d__:Domain {name: domain_name})
                ON CREATE SET __cy_d__.description = ''
                MERGE (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__)
            )
            FOREACH (owner_name IN coalesce(term.owners, []) |
                MERGE (__cy_o__:Owner {name: owner_name})
                ON CREATE SET __cy_o__.email = '', __cy_o__.role = 'Owner'
                MERGE (__cy_t__)-[:OWNED_BY]->(__cy_o__)
            )
            FOREACH (reviewer_name IN coalesce(term.reviewers, []) |
                MERGE (__cy_r__:Owner {name: reviewer_name})
                ON CREATE SET __cy_r__.email = '', __cy_r__.role = 'Reviewer'
                MERGE (__cy_t__)-[:REVIEWED_BY]->(__cy_r__)
            )
            FOREACH (tag_name IN coalesce(term.tags, []) |
                MERGE (__cy_tag__:Tag {name: tag_name})
                ON CREATE SET __cy_tag__.color = '#3498db'
                MERGE (__cy_t__)-[:HAS_TAG]->(__cy_tag__)
            )
            RETURN term.name AS name, 'created' AS result
        """,
        "parameters": {
            "glossary_id": glossary_id,
            "terms": terms,
            "batch_id": batch_id,
            "timestamp": _utc_now_iso(),
        },
    }
    async with Neo4jClient() as client:
        result = await client.execute_queries([query])
    return result[0] if result else []


async def bulk_create_terms(
    glossary_id: str,
    terms: list[dict[str, Any]],
    unmapped_columns_reminder: Optional[list[str]] = None,
) -> dict[str, Any]:
    batch_id = str(uuid.uuid4())
    total_created = 0
    total_skipped = 0
    errors: list[dict[str, str]] = []
    domains_created: set[str] = set()
    domains_existing: set[str] = set()
    seen_names: set[str] = set()

    for chunk_start in range(0, len(terms), DEFAULT_IMPORT_CHUNK_SIZE):
        chunk = terms[chunk_start:chunk_start + DEFAULT_IMPORT_CHUNK_SIZE]
        sanitized_chunk = [_sanitize_import_term(term, batch_id) for term in chunk]

        chunk_names = _dedupe_strings(item["name"] for item in sanitized_chunk)
        existing_names = await _fetch_existing_term_names(glossary_id, chunk_names)
        creatable_terms: list[dict[str, Any]] = []

        for term in sanitized_chunk:
            if not term["name"]:
                total_skipped += 1
                errors.append({"name": "", "reason": "용어 이름이 비어 있습니다."})
                continue
            if term["name"] in existing_names or term["name"] in seen_names:
                total_skipped += 1
                errors.append({"name": term["name"], "reason": "동일한 이름의 용어가 이미 존재합니다."})
                continue
            seen_names.add(term["name"])
            creatable_terms.append(term)

        chunk_domain_names = _dedupe_strings(domain for item in creatable_terms for domain in item["domains"])
        existing_domains_before_chunk = await _fetch_existing_domain_names(chunk_domain_names)
        try:
            inserted_rows = await _insert_term_chunk(glossary_id, batch_id, creatable_terms)
        except Exception as exc:
            total_skipped += len(creatable_terms)
            for term in creatable_terms:
                errors.append({
                    "name": term["name"],
                    "reason": f"청크 삽입 실패: {exc}",
                })
            continue

        inserted_names = {row["name"] for row in inserted_rows if row.get("result") == "created" and row.get("name")}
        if len(inserted_names) != len(creatable_terms):
            missing_names = [term["name"] for term in creatable_terms if term["name"] not in inserted_names]
            for name in missing_names:
                total_skipped += 1
                errors.append({"name": name, "reason": "배치 삽입 결과를 확인하지 못했습니다."})

        total_created += len(inserted_names)

        for term in creatable_terms:
            if term["name"] not in inserted_names:
                continue
            for domain in term["domains"]:
                if domain in existing_domains_before_chunk or domain in domains_created:
                    domains_existing.add(domain)
                else:
                    domains_created.add(domain)

    return {
        "batch_id": batch_id,
        "created": total_created,
        "skipped": total_skipped,
        "errors": errors,
        "domains_created": len(domains_created),
        "domains_existing": len(domains_existing),
        "unmapped_columns_reminder": _dedupe_strings(unmapped_columns_reminder or []),
        "next_steps": [
            "용어 목록에서 가져온 용어를 확인할 수 있습니다.",
            "매핑되지 않은 칼럼 데이터는 별도 편집으로 보완할 수 있습니다.",
            "문제가 있다면 이 배치 되돌리기로 일괄 삭제할 수 있습니다.",
        ],
    }


async def rollback_batch(glossary_id: str, batch_id: str) -> dict[str, Any]:
    query = {
        "query": """
            MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term {batch_id: $batch_id})
            WHERE elementId(__cy_g__) = $glossary_id
            WITH collect(__cy_t__) AS terms
            FOREACH (term IN terms | DETACH DELETE term)
            RETURN size(terms) AS deleted
        """,
        "parameters": {"glossary_id": glossary_id, "batch_id": batch_id},
    }
    async with Neo4jClient() as client:
        result = await client.execute_queries([query])
    deleted = result[0][0]["deleted"] if result and result[0] else 0
    return {"deleted": deleted, "batch_id": batch_id}
