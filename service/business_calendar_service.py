"""영업일 캘린더 관리 서비스

Neo4j 기반으로 영업일/비영업일 데이터를 관리합니다.
타임시리즈 데이터 분석 시 비영업일을 필터아웃하기 위한 기준 데이터입니다.

스키마:
- BusinessCalendar: 연도별 영업일 캘린더 설정
- NonBusinessDay: 비영업일 날짜
- Holiday: 공휴일

관계:
- (BusinessCalendar)-[:HAS_NON_BUSINESS_DAY]->(NonBusinessDay)
- (BusinessCalendar)-[:HAS_HOLIDAY]->(Holiday)
"""

import logging
from typing import List, Dict, Any
from datetime import datetime

from client.neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)


def get_current_timestamp() -> str:
    return datetime.utcnow().isoformat() + "Z"


# =============================================================================
# 영업일 캘린더 조회
# =============================================================================

async def fetch_business_days(year: int) -> dict:
    """특정 연도의 영업일 설정 조회

    Returns:
        {
            "businessDays": [{"date": "2026-01-01", "isBusinessDay": false, "reason": "신정"}, ...],
            "holidays": [{"date": "2026-01-01", "name": "신정"}, ...],
            "settings": {"excludeWeekends": true}
        }
    """
    client = Neo4jClient()
    try:
        settings_query = {
            "query": """
                MATCH (cal:BusinessCalendar {year: $year})
                RETURN cal.excludeWeekends AS excludeWeekends,
                       cal.createdAt AS createdAt,
                       cal.updatedAt AS updatedAt
            """,
            "parameters": {"year": year}
        }

        non_biz_query = {
            "query": """
                MATCH (cal:BusinessCalendar {year: $year})-[:HAS_NON_BUSINESS_DAY]->(nbd:NonBusinessDay)
                RETURN nbd.date AS date, nbd.reason AS reason
                ORDER BY nbd.date
            """,
            "parameters": {"year": year}
        }

        holiday_query = {
            "query": """
                MATCH (cal:BusinessCalendar {year: $year})-[:HAS_HOLIDAY]->(h:Holiday)
                RETURN h.date AS date, h.name AS name
                ORDER BY h.date
            """,
            "parameters": {"year": year}
        }

        results = await client.execute_queries([settings_query, non_biz_query, holiday_query])

        settings_data = {"excludeWeekends": True}
        if results[0]:
            record = results[0][0]
            settings_data["excludeWeekends"] = record.get("excludeWeekends", True)

        business_days = []
        for record in (results[1] or []):
            business_days.append({
                "date": record["date"],
                "isBusinessDay": False,
                "reason": record.get("reason", "")
            })

        holidays = []
        for record in (results[2] or []):
            holidays.append({
                "date": record["date"],
                "name": record.get("name", "")
            })

        return {
            "businessDays": business_days,
            "holidays": holidays,
            "settings": settings_data
        }

    finally:
        await client.close()


# =============================================================================
# 영업일 캘린더 저장
# =============================================================================

async def save_business_days(
    year: int,
    non_business_days: List[Dict[str, Any]],
    holidays: List[Dict[str, str]],
    settings: Dict[str, Any]
) -> dict:
    """영업일 설정 저장 (기존 데이터 덮어쓰기)

    Args:
        year: 연도
        non_business_days: 비영업일 목록 [{"date": "...", "isBusinessDay": false, "reason": "..."}]
        holidays: 공휴일 목록 [{"date": "...", "name": "..."}]
        settings: 설정 {"excludeWeekends": true}

    Returns:
        {"success": true}
    """
    timestamp = get_current_timestamp()
    exclude_weekends = settings.get("excludeWeekends", True)

    client = Neo4jClient()
    try:
        queries = []

        # 1. 기존 데이터 삭제
        queries.append({
            "query": """
                MATCH (cal:BusinessCalendar {year: $year})
                OPTIONAL MATCH (cal)-[:HAS_NON_BUSINESS_DAY]->(nbd:NonBusinessDay)
                OPTIONAL MATCH (cal)-[:HAS_HOLIDAY]->(h:Holiday)
                DETACH DELETE nbd, h, cal
            """,
            "parameters": {"year": year}
        })

        # 2. 캘린더 노드 생성
        queries.append({
            "query": """
                CREATE (cal:BusinessCalendar {
                    year: $year,
                    excludeWeekends: $excludeWeekends,
                    createdAt: $timestamp,
                    updatedAt: $timestamp
                })
                RETURN elementId(cal) AS id
            """,
            "parameters": {
                "year": year,
                "excludeWeekends": exclude_weekends,
                "timestamp": timestamp
            }
        })

        # 3. 비영업일 노드 생성
        if non_business_days:
            queries.append({
                "query": """
                    MATCH (cal:BusinessCalendar {year: $year})
                    UNWIND $days AS day
                    CREATE (nbd:NonBusinessDay {
                        date: day.date,
                        reason: COALESCE(day.reason, '수동 지정'),
                        createdAt: $timestamp
                    })
                    CREATE (cal)-[:HAS_NON_BUSINESS_DAY]->(nbd)
                """,
                "parameters": {
                    "year": year,
                    "days": non_business_days,
                    "timestamp": timestamp
                }
            })

        # 4. 공휴일 노드 생성
        if holidays:
            queries.append({
                "query": """
                    MATCH (cal:BusinessCalendar {year: $year})
                    UNWIND $holidays AS holiday
                    CREATE (h:Holiday {
                        date: holiday.date,
                        name: holiday.name,
                        createdAt: $timestamp
                    })
                    CREATE (cal)-[:HAS_HOLIDAY]->(h)
                """,
                "parameters": {
                    "year": year,
                    "holidays": holidays,
                    "timestamp": timestamp
                }
            })

        await client.execute_queries(queries)

        logger.info(
            "[BusinessCalendar] 저장 완료 | year=%d | non_biz=%d | holidays=%d",
            year, len(non_business_days), len(holidays)
        )

        return {"success": True}

    finally:
        await client.close()


# =============================================================================
# 영업일 확인
# =============================================================================

async def check_business_day(date_str: str) -> dict:
    """특정 날짜가 영업일인지 확인

    Args:
        date_str: YYYY-MM-DD 형식의 날짜

    Returns:
        {"date": "2026-01-01", "isBusinessDay": false}
    """
    year = int(date_str[:4])

    client = Neo4jClient()
    try:
        query = {
            "query": """
                OPTIONAL MATCH (cal:BusinessCalendar {year: $year})-[:HAS_NON_BUSINESS_DAY]->(nbd:NonBusinessDay {date: $date})
                OPTIONAL MATCH (cal2:BusinessCalendar {year: $year})-[:HAS_HOLIDAY]->(h:Holiday {date: $date})
                OPTIONAL MATCH (cal3:BusinessCalendar {year: $year})
                RETURN
                    nbd IS NOT NULL AS isNonBusinessDay,
                    h IS NOT NULL AS isHoliday,
                    cal3.excludeWeekends AS excludeWeekends
            """,
            "parameters": {"year": year, "date": date_str}
        }

        results = await client.execute_queries([query])

        if results and results[0]:
            record = results[0][0]
            is_non_biz = record.get("isNonBusinessDay", False)
            is_holiday = record.get("isHoliday", False)
            exclude_weekends = record.get("excludeWeekends", True)

            is_weekend = False
            if exclude_weekends:
                d = datetime.strptime(date_str, "%Y-%m-%d")
                is_weekend = d.weekday() >= 5  # 토(5), 일(6)

            is_business_day = not (is_non_biz or is_holiday or is_weekend)

            return {"date": date_str, "isBusinessDay": is_business_day}

        return {"date": date_str, "isBusinessDay": True}

    finally:
        await client.close()


# =============================================================================
# 영업일 설정 삭제
# =============================================================================

async def delete_business_days(year: int) -> dict:
    """특정 연도의 영업일 설정 삭제

    Returns:
        {"success": true}
    """
    client = Neo4jClient()
    try:
        query = {
            "query": """
                MATCH (cal:BusinessCalendar {year: $year})
                OPTIONAL MATCH (cal)-[:HAS_NON_BUSINESS_DAY]->(nbd:NonBusinessDay)
                OPTIONAL MATCH (cal)-[:HAS_HOLIDAY]->(h:Holiday)
                DETACH DELETE nbd, h, cal
            """,
            "parameters": {"year": year}
        }

        await client.execute_queries([query])

        logger.info("[BusinessCalendar] 삭제 완료 | year=%d", year)
        return {"success": True}

    finally:
        await client.close()


# =============================================================================
# 비영업일 날짜 목록 조회 (필터링용)
# =============================================================================

async def fetch_non_business_dates(start_date: str, end_date: str) -> dict:
    """기간 내 비영업일 날짜 목록 조회 (타임시리즈 필터용)

    Args:
        start_date: 시작 날짜 (YYYY-MM-DD)
        end_date: 종료 날짜 (YYYY-MM-DD)

    Returns:
        {"dates": ["2026-01-01", "2026-01-04", ...]}
    """
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    client = Neo4jClient()
    try:
        non_business_dates = set()

        for year in range(start_year, end_year + 1):
            nbd_query = {
                "query": """
                    MATCH (cal:BusinessCalendar {year: $year})-[:HAS_NON_BUSINESS_DAY]->(nbd:NonBusinessDay)
                    WHERE nbd.date >= $startDate AND nbd.date <= $endDate
                    RETURN nbd.date AS date
                """,
                "parameters": {"year": year, "startDate": start_date, "endDate": end_date}
            }

            hol_query = {
                "query": """
                    MATCH (cal:BusinessCalendar {year: $year})-[:HAS_HOLIDAY]->(h:Holiday)
                    WHERE h.date >= $startDate AND h.date <= $endDate
                    RETURN h.date AS date
                """,
                "parameters": {"year": year, "startDate": start_date, "endDate": end_date}
            }

            settings_query = {
                "query": """
                    MATCH (cal:BusinessCalendar {year: $year})
                    RETURN cal.excludeWeekends AS excludeWeekends
                """,
                "parameters": {"year": year}
            }

            results = await client.execute_queries([nbd_query, hol_query, settings_query])

            for record in (results[0] or []):
                non_business_dates.add(record["date"])

            for record in (results[1] or []):
                non_business_dates.add(record["date"])

            exclude_weekends = True
            if results[2]:
                exclude_weekends = results[2][0].get("excludeWeekends", True)

            if exclude_weekends:
                from datetime import timedelta
                current = datetime.strptime(max(start_date, f"{year}-01-01"), "%Y-%m-%d")
                end = datetime.strptime(min(end_date, f"{year}-12-31"), "%Y-%m-%d")
                while current <= end:
                    if current.weekday() >= 5:  # 토, 일
                        non_business_dates.add(current.strftime("%Y-%m-%d"))
                    current += timedelta(days=1)

        sorted_dates = sorted(non_business_dates)

        return {"dates": sorted_dates}

    finally:
        await client.close()
