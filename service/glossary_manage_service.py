"""용어집 관리 서비스

비즈니스 용어집 CRUD 기능을 제공합니다.

주요 기능:
- 용어집(Glossary) CRUD
- 용어(Term) CRUD
- 도메인/소유자/태그 관리
"""

import logging
from typing import Optional, List
from datetime import datetime

from client.neo4j_client import Neo4jClient


logger = logging.getLogger(__name__)


def get_current_timestamp() -> str:
    """현재 시간을 ISO 형식으로 반환"""
    return datetime.utcnow().isoformat() + "Z"


def _normalize_unique_names(values: Optional[List[str]]) -> List[str]:
    """이름 목록을 정리하고 중복을 제거"""
    if values is None or not isinstance(values, list):
        return []

    normalized: List[str] = []
    seen: set[str] = set()

    for value in values:
        if not isinstance(value, str):
            continue

        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue

        seen.add(cleaned)
        normalized.append(cleaned)

    return normalized


def _clean_collected_names(values: Optional[List[Optional[str]]]) -> List[str]:
    """Cypher collect 결과에서 빈 값을 제거"""
    if not values:
        return []

    cleaned: List[str] = []
    seen: set[str] = set()

    for value in values:
        if not isinstance(value, str):
            continue

        normalized = value.strip()
        if not normalized or normalized in seen:
            continue

        seen.add(normalized)
        cleaned.append(normalized)

    return cleaned


def _clean_collected_tags(values: Optional[List[Optional[dict]]]) -> List[dict]:
    """Cypher collect 결과에서 유효한 태그 객체만 반환"""
    if not values:
        return []

    cleaned: List[dict] = []
    seen: set[str] = set()

    for value in values:
        if not isinstance(value, dict):
            continue

        raw_name = value.get("name")
        if not isinstance(raw_name, str):
            continue

        name = raw_name.strip()
        if not name or name in seen:
            continue

        seen.add(name)
        cleaned.append({
            "id": value.get("id") or name,
            "name": name,
            "color": value.get("color") or "#3498db",
        })

    return cleaned


def _build_term_metadata_sync_fragment(term_var: str, term_data: dict) -> tuple[str, dict]:
    """단일 Cypher 쿼리 안에서 메타데이터 관계를 동기화하는 fragment 생성"""
    fragments: List[str] = []
    params: dict = {}

    relation_specs = [
        {
            "field_name": "domains",
            "param_name": "domains",
            "delete_alias": "__cy_domain_rel__",
            "relation_type": "BELONGS_TO_DOMAIN",
            "node_label": "Domain",
            "node_alias": "__cy_domain__",
            "on_create_set": "ON CREATE SET __cy_domain__.description = ''",
        },
        {
            "field_name": "owners",
            "param_name": "owners",
            "delete_alias": "__cy_owner_rel__",
            "relation_type": "OWNED_BY",
            "node_label": "Owner",
            "node_alias": "__cy_owner__",
            "on_create_set": "ON CREATE SET __cy_owner__.email = '', __cy_owner__.role = 'Owner'",
        },
        {
            "field_name": "reviewers",
            "param_name": "reviewers",
            "delete_alias": "__cy_reviewer_rel__",
            "relation_type": "REVIEWED_BY",
            "node_label": "Owner",
            "node_alias": "__cy_reviewer__",
            "on_create_set": "ON CREATE SET __cy_reviewer__.email = '', __cy_reviewer__.role = 'Reviewer'",
        },
        {
            "field_name": "tags",
            "param_name": "tags",
            "delete_alias": "__cy_tag_rel__",
            "relation_type": "HAS_TAG",
            "node_label": "Tag",
            "node_alias": "__cy_tag__",
            "on_create_set": "ON CREATE SET __cy_tag__.color = '#3498db'",
        },
    ]

    for spec in relation_specs:
        raw_values = term_data.get(spec["field_name"])
        if raw_values is None:
            continue

        params[spec["param_name"]] = _normalize_unique_names(raw_values)
        fragments.extend([
            f"WITH {term_var}",
            f"OPTIONAL MATCH ({term_var})-[{spec['delete_alias']}:{spec['relation_type']}]->(:{spec['node_label']})",
            f"DELETE {spec['delete_alias']}",
            f"WITH {term_var}",
            f"FOREACH (item_name IN ${spec['param_name']} |",
            f"    MERGE ({spec['node_alias']}:{spec['node_label']} {{name: item_name}})",
            f"    {spec['on_create_set']}",
            f"    MERGE ({term_var})-[:{spec['relation_type']}]->({spec['node_alias']})",
            ")",
        ])

    return ("\n".join(fragments), params)


# =============================================================================
# 용어집(Glossary) CRUD
# =============================================================================

async def fetch_all_glossaries() -> dict:
    """모든 용어집 목록 조회
    
    Returns:
        {"glossaries": [...]}
    """
    query = """
        MATCH (__cy_g__:Glossary)
        OPTIONAL MATCH (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term)
        WITH __cy_g__, count(__cy_t__) as termCount
        RETURN 
            elementId(__cy_g__) as id,
            __cy_g__.name as name,
            __cy_g__.description as description,
            __cy_g__.type as type,
            __cy_g__.created_at as createdAt,
            __cy_g__.updated_at as updatedAt,
            termCount
        ORDER BY __cy_g__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        glossaries = []
        for record in result[0] if result else []:
            glossaries.append({
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description", ""),
                "type": record.get("type", "Business"),
                "termCount": record.get("termCount", 0),
                "createdAt": record.get("createdAt"),
                "updatedAt": record.get("updatedAt"),
            })
        return {"glossaries": glossaries}
    finally:
        await client.close()


async def create_new_glossary(name: str, description: str, type_: str) -> dict:
    """용어집 생성
    
    Returns:
        생성된 용어집 정보
    """
    timestamp = get_current_timestamp()
    
    query = {
        "query": """
            CREATE (__cy_g__:Glossary {
                name: $name,
                description: $description,
                type: $type,
                created_at: $timestamp,
                updated_at: $timestamp
            })
            RETURN elementId(__cy_g__) as id, __cy_g__.name as name
        """,
        "parameters": {
            "name": name,
            "description": description,
            "type": type_,
            "timestamp": timestamp,
        }
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            record = result[0][0]
            return {
                "id": record["id"],
                "name": record["name"],
                "message": "용어집이 생성되었습니다."
            }
        raise RuntimeError("용어집 생성 실패")
    finally:
        await client.close()


async def fetch_glossary_by_id(glossary_id: str) -> dict:
    """용어집 상세 조회
    
    Returns:
        용어집 정보
    """
    query = {
        "query": """
            MATCH (__cy_g__:Glossary)
            WHERE elementId(__cy_g__) = $glossary_id
            OPTIONAL MATCH (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term)
            WITH __cy_g__, count(__cy_t__) as termCount
            RETURN 
                elementId(__cy_g__) as id,
                __cy_g__.name as name,
                __cy_g__.description as description,
                __cy_g__.type as type,
                __cy_g__.created_at as createdAt,
                __cy_g__.updated_at as updatedAt,
                termCount
        """,
        "parameters": {"glossary_id": glossary_id}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            record = result[0][0]
            return {
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description", ""),
                "type": record.get("type", "Business"),
                "termCount": record.get("termCount", 0),
                "createdAt": record.get("createdAt"),
                "updatedAt": record.get("updatedAt"),
            }
        return None
    finally:
        await client.close()


async def update_glossary_info(
    glossary_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    type_: Optional[str] = None
) -> dict:
    """용어집 수정
    
    Returns:
        수정 결과
    """
    set_clauses = ["__cy_g__.updated_at = $timestamp"]
    params = {"glossary_id": glossary_id, "timestamp": get_current_timestamp()}
    
    if name is not None:
        set_clauses.append("__cy_g__.name = $name")
        params["name"] = name
    if description is not None:
        set_clauses.append("__cy_g__.description = $description")
        params["description"] = description
    if type_ is not None:
        set_clauses.append("__cy_g__.type = $type")
        params["type"] = type_
    
    query = {
        "query": f"""
            MATCH (__cy_g__:Glossary)
            WHERE elementId(__cy_g__) = $glossary_id
            SET {', '.join(set_clauses)}
            RETURN elementId(__cy_g__) as id
        """,
        "parameters": params
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"message": "용어집이 수정되었습니다.", "updated": True}
        return {"message": "용어집을 찾을 수 없습니다.", "updated": False}
    finally:
        await client.close()


async def delete_glossary_by_id(glossary_id: str) -> dict:
    """용어집 삭제
    
    Returns:
        삭제 결과
    """
    query = {
        "query": """
            MATCH (__cy_g__:Glossary)
            WHERE elementId(__cy_g__) = $glossary_id
            OPTIONAL MATCH (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term)
            DETACH DELETE __cy_g__, __cy_t__
            RETURN count(*) as deleted
        """,
        "parameters": {"glossary_id": glossary_id}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        return {"message": "용어집이 삭제되었습니다.", "deleted": True}
    finally:
        await client.close()


# =============================================================================
# 용어(Term) CRUD
# =============================================================================

async def fetch_glossary_terms(
    glossary_id: str,
    search: Optional[str] = None,
    limit: int = 100
) -> dict:
    """용어 목록 조회
    
    Returns:
        {"terms": [...]}
    """
    params = {"glossary_id": glossary_id, "limit": limit}
    where_clause = "elementId(__cy_g__) = $glossary_id"
    
    if search:
        where_clause += " AND (toLower(__cy_t__.name) CONTAINS toLower($search))"
        params["search"] = search
    
    query = {
        "query": f"""
            MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
            WHERE {where_clause}
            OPTIONAL MATCH (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__:Domain)
            OPTIONAL MATCH (__cy_t__)-[:HAS_TAG]->(__cy_tag__:Tag)
            OPTIONAL MATCH (__cy_t__)-[:OWNED_BY]->(__cy_o__:Owner)
            OPTIONAL MATCH (__cy_t__)-[:REVIEWED_BY]->(__cy_r__:Owner)
            RETURN 
                elementId(__cy_t__) as id,
                __cy_t__.name as name,
                __cy_t__.description as description,
                __cy_t__.status as status,
                __cy_t__.synonyms as synonyms,
                collect(DISTINCT __cy_d__.name) as domains,
                collect(DISTINCT CASE
                    WHEN __cy_tag__ IS NULL THEN NULL
                    ELSE {{
                        id: elementId(__cy_tag__),
                        name: __cy_tag__.name,
                        color: coalesce(__cy_tag__.color, '#3498db')
                    }}
                END) as tags,
                collect(DISTINCT __cy_o__.name) as owners,
                collect(DISTINCT __cy_r__.name) as reviewers
            ORDER BY __cy_t__.name
            LIMIT $limit
        """,
        "parameters": params
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        terms = []
        for record in result[0] if result else []:
            terms.append({
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description", ""),
                "status": record.get("status", "Draft"),
                "synonyms": record.get("synonyms", []),
                "domains": _clean_collected_names(record.get("domains")),
                "tags": _clean_collected_tags(record.get("tags")),
                "owners": _clean_collected_names(record.get("owners")),
                "reviewers": _clean_collected_names(record.get("reviewers")),
            })
        return {"terms": terms}
    finally:
        await client.close()


async def create_new_term(glossary_id: str, term_data: dict) -> dict:
    """용어 생성
    
    Returns:
        생성된 용어 정보
    """
    timestamp = get_current_timestamp()
    name = term_data.get("name", "")
    description = term_data.get("description", "")
    status = term_data.get("status", "Draft")
    synonyms = _normalize_unique_names(term_data.get("synonyms"))
    batch_id = term_data.get("batch_id")
    metadata_fragment, metadata_params = _build_term_metadata_sync_fragment("__cy_t__", {
        "domains": term_data.get("domains", []),
        "owners": term_data.get("owners", []),
        "reviewers": term_data.get("reviewers", []),
        "tags": term_data.get("tags", []),
    })
    
    query = {
        "query": f"""
            MATCH (__cy_g__:Glossary)
            WHERE elementId(__cy_g__) = $glossary_id
            CREATE (__cy_g__)-[:HAS_TERM]->(__cy_t__:Term {{
                name: $name,
                description: $description,
                status: $status,
                synonyms: $synonyms,
                batch_id: $batch_id,
                created_at: $timestamp,
                updated_at: $timestamp
            }})
            {metadata_fragment}
            RETURN elementId(__cy_t__) as id, __cy_t__.name as name
        """,
        "parameters": {
            "glossary_id": glossary_id,
            "name": name,
            "description": description,
            "status": status,
            "synonyms": synonyms,
            "batch_id": batch_id,
            "timestamp": timestamp,
            **metadata_params,
        }
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            record = result[0][0]
            return {
                "id": record["id"],
                "name": record["name"],
                "message": "용어가 생성되었습니다."
            }
        raise RuntimeError("용어 생성 실패")
    finally:
        await client.close()


async def fetch_term_by_id(glossary_id: str, term_id: str) -> dict:
    """용어 상세 조회
    
    Returns:
        용어 정보
    """
    query = {
        "query": """
            MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
            WHERE elementId(__cy_g__) = $glossary_id
              AND elementId(__cy_t__) = $term_id
            OPTIONAL MATCH (__cy_t__)-[:BELONGS_TO_DOMAIN]->(__cy_d__:Domain)
            OPTIONAL MATCH (__cy_t__)-[:HAS_TAG]->(__cy_tag__:Tag)
            OPTIONAL MATCH (__cy_t__)-[:OWNED_BY]->(__cy_o__:Owner)
            OPTIONAL MATCH (__cy_t__)-[:REVIEWED_BY]->(__cy_r__:Owner)
            RETURN 
                elementId(__cy_t__) as id,
                __cy_t__.name as name,
                __cy_t__.description as description,
                __cy_t__.status as status,
                __cy_t__.synonyms as synonyms,
                collect(DISTINCT __cy_d__.name) as domains,
                collect(DISTINCT CASE
                    WHEN __cy_tag__ IS NULL THEN NULL
                    ELSE {
                        id: elementId(__cy_tag__),
                        name: __cy_tag__.name,
                        color: coalesce(__cy_tag__.color, '#3498db')
                    }
                END) as tags,
                collect(DISTINCT __cy_o__.name) as owners,
                collect(DISTINCT __cy_r__.name) as reviewers
        """,
        "parameters": {"glossary_id": glossary_id, "term_id": term_id}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            record = result[0][0]
            return {
                "id": record["id"],
                "name": record["name"],
                "description": record.get("description", ""),
                "status": record.get("status", "Draft"),
                "synonyms": record.get("synonyms", []),
                "domains": _clean_collected_names(record.get("domains")),
                "tags": _clean_collected_tags(record.get("tags")),
                "owners": _clean_collected_names(record.get("owners")),
                "reviewers": _clean_collected_names(record.get("reviewers")),
            }
        return None
    finally:
        await client.close()


async def update_term_info(glossary_id: str, term_id: str, term_data: dict) -> dict:
    """용어 수정
    
    Returns:
        수정 결과
    """
    set_clauses = ["__cy_t__.updated_at = $timestamp"]
    params = {"glossary_id": glossary_id, "term_id": term_id, "timestamp": get_current_timestamp()}
    
    if term_data.get("name") is not None:
        set_clauses.append("__cy_t__.name = $name")
        params["name"] = term_data["name"]
    if term_data.get("description") is not None:
        set_clauses.append("__cy_t__.description = $description")
        params["description"] = term_data["description"]
    if term_data.get("status") is not None:
        set_clauses.append("__cy_t__.status = $status")
        params["status"] = term_data["status"]
    if term_data.get("synonyms") is not None:
        set_clauses.append("__cy_t__.synonyms = $synonyms")
        params["synonyms"] = _normalize_unique_names(term_data["synonyms"])

    metadata_fragment, metadata_params = _build_term_metadata_sync_fragment("__cy_t__", term_data)
    
    query = {
        "query": f"""
            MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
            WHERE elementId(__cy_g__) = $glossary_id
              AND elementId(__cy_t__) = $term_id
            SET {', '.join(set_clauses)}
            {metadata_fragment}
            RETURN elementId(__cy_t__) as id
        """,
        "parameters": {
            **params,
            **metadata_params,
        }
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"message": "용어가 수정되었습니다.", "updated": True}
        return {"message": "용어를 찾을 수 없습니다.", "updated": False}
    finally:
        await client.close()


async def delete_term_by_id(glossary_id: str, term_id: str) -> dict:
    """용어 삭제
    
    Returns:
        삭제 결과
    """
    query = {
        "query": """
            MATCH (__cy_g__:Glossary)-[:HAS_TERM]->(__cy_t__:Term)
            WHERE elementId(__cy_g__) = $glossary_id
              AND elementId(__cy_t__) = $term_id
            DETACH DELETE __cy_t__
            RETURN count(*) as deleted
        """,
        "parameters": {"glossary_id": glossary_id, "term_id": term_id}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        return {"message": "용어가 삭제되었습니다.", "deleted": True}
    finally:
        await client.close()


# =============================================================================
# 도메인/소유자/태그 관리
# =============================================================================

async def fetch_all_domains() -> dict:
    """도메인 목록 조회"""
    query = """
        MATCH (__cy_d__:Domain)
        RETURN elementId(__cy_d__) as id, __cy_d__.name as name, __cy_d__.description as description
        ORDER BY __cy_d__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        domains = [{"id": r["id"], "name": r["name"], "description": r.get("description", "")} 
                   for r in (result[0] if result else [])]
        return {"domains": domains}
    finally:
        await client.close()


async def fetch_all_owners() -> dict:
    """소유자 목록 조회"""
    query = """
        MATCH (__cy_o__:Owner)
        RETURN elementId(__cy_o__) as id, __cy_o__.name as name, __cy_o__.email as email, __cy_o__.role as role
        ORDER BY __cy_o__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        owners = [{"id": r["id"], "name": r["name"], "email": r.get("email", ""), "role": r.get("role", "Owner")} 
                  for r in (result[0] if result else [])]
        return {"owners": owners}
    finally:
        await client.close()


async def fetch_all_tags() -> dict:
    """태그 목록 조회"""
    query = """
        MATCH (__cy_tag__:Tag)
        RETURN elementId(__cy_tag__) as id, __cy_tag__.name as name, __cy_tag__.color as color
        ORDER BY __cy_tag__.name
    """
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        tags = [{"id": r["id"], "name": r["name"], "color": r.get("color", "#3498db")} 
                for r in (result[0] if result else [])]
        return {"tags": tags}
    finally:
        await client.close()


async def create_new_domain(name: str, description: str = "") -> dict:
    """도메인 생성"""
    query = {
        "query": """
            CREATE (__cy_d__:Domain {name: $name, description: $description})
            RETURN elementId(__cy_d__) as id, __cy_d__.name as name
        """,
        "parameters": {"name": name, "description": description}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"id": result[0][0]["id"], "name": result[0][0]["name"], "message": "도메인이 생성되었습니다."}
        raise RuntimeError("도메인 생성 실패")
    finally:
        await client.close()


async def create_new_owner(name: str, email: str, role: str) -> dict:
    """소유자 생성"""
    query = {
        "query": """
            CREATE (__cy_o__:Owner {name: $name, email: $email, role: $role})
            RETURN elementId(__cy_o__) as id, __cy_o__.name as name
        """,
        "parameters": {"name": name, "email": email, "role": role}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {"id": result[0][0]["id"], "name": result[0][0]["name"], "message": "소유자가 생성되었습니다."}
        raise RuntimeError("소유자 생성 실패")
    finally:
        await client.close()


async def create_new_tag(name: str, color: str) -> dict:
    """태그 생성"""
    query = {
        "query": """
            CREATE (__cy_tag__:Tag {name: $name, color: $color})
            RETURN elementId(__cy_tag__) as id, __cy_tag__.name as name, __cy_tag__.color as color
        """,
        "parameters": {"name": name, "color": color}
    }
    
    client = Neo4jClient()
    try:
        result = await client.execute_queries([query])
        if result and result[0]:
            return {
                "id": result[0][0]["id"],
                "name": result[0][0]["name"],
                "color": result[0][0].get("color", color),
                "message": "태그가 생성되었습니다.",
            }
        raise RuntimeError("태그 생성 실패")
    finally:
        await client.close()

