"""영업일 캘린더 관리 API 라우터

타임시리즈 데이터 분석을 위한 영업일/비영업일 관리 기능을 제공합니다.

스키마:
- BusinessCalendar: 연도별 영업일 캘린더 설정
- NonBusinessDay: 비영업일 날짜
- Holiday: 공휴일

관계:
- (BusinessCalendar)-[:HAS_NON_BUSINESS_DAY]->(NonBusinessDay)
- (BusinessCalendar)-[:HAS_HOLIDAY]->(Holiday)
"""

import logging
from typing import List

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from config.settings import settings
from service import business_calendar_service


router = APIRouter(prefix=f"{settings.api_prefix}/business-calendar")
logger = logging.getLogger(__name__)


# =============================================================================
# 요청/응답 모델
# =============================================================================

class HolidayItem(BaseModel):
    """공휴일 항목"""
    date: str = Field(..., description="날짜 (YYYY-MM-DD)")
    name: str = Field(..., description="공휴일명")


class NonBusinessDayItem(BaseModel):
    """비영업일 항목"""
    date: str = Field(..., description="날짜 (YYYY-MM-DD)")
    isBusinessDay: bool = Field(False, description="영업일 여부")
    reason: str = Field("수동 지정", description="비영업일 사유")


class CalendarSettings(BaseModel):
    """캘린더 설정"""
    excludeWeekends: bool = Field(True, description="주말(토/일)을 비영업일로 처리")


class SaveBusinessDaysRequest(BaseModel):
    """영업일 설정 저장 요청"""
    year: int = Field(..., description="연도")
    nonBusinessDays: List[NonBusinessDayItem] = Field(default_factory=list, description="비영업일 목록")
    holidays: List[HolidayItem] = Field(default_factory=list, description="공휴일 목록")
    settings: CalendarSettings = Field(default_factory=CalendarSettings, description="캘린더 설정")


@router.get("/check/{date}")
async def check_business_day(date: str):
    """특정 날짜가 영업일인지 확인"""
    logger.info("[API] 영업일 확인 | date=%s", date)
    try:
        return await business_calendar_service.check_business_day(date)
    except Exception as e:
        logger.error("[API] 영업일 확인 실패 | error=%s", e)
        raise HTTPException(500, f"영업일 확인 실패: {e}")


@router.get("/non-business-dates")
async def get_non_business_dates(start: str, end: str):
    """기간 내 비영업일 날짜 목록 조회 (타임시리즈 필터용)

    Query Params:
        start: 시작일 (YYYY-MM-DD)
        end: 종료일 (YYYY-MM-DD)
    """
    logger.info("[API] 비영업일 조회 | start=%s | end=%s", start, end)
    try:
        return await business_calendar_service.fetch_non_business_dates(start, end)
    except Exception as e:
        logger.error("[API] 비영업일 조회 실패 | error=%s", e)
        raise HTTPException(500, f"비영업일 조회 실패: {e}")


@router.get("/{year}")
async def get_business_days(year: int):
    """특정 연도의 영업일 설정 조회"""
    logger.info("[API] 영업일 조회 | year=%d", year)
    try:
        return await business_calendar_service.fetch_business_days(year)
    except Exception as e:
        logger.error("[API] 영업일 조회 실패 | error=%s", e)
        raise HTTPException(500, f"영업일 조회 실패: {e}")


@router.post("/")
async def save_business_days(body: SaveBusinessDaysRequest):
    """영업일 설정 저장"""
    logger.info("[API] 영업일 저장 | year=%d | days=%d | holidays=%d",
                body.year, len(body.nonBusinessDays), len(body.holidays))
    try:
        return await business_calendar_service.save_business_days(
            year=body.year,
            non_business_days=[d.model_dump() for d in body.nonBusinessDays],
            holidays=[h.model_dump() for h in body.holidays],
            settings=body.settings.model_dump(),
        )
    except Exception as e:
        logger.error("[API] 영업일 저장 실패 | error=%s", e)
        raise HTTPException(500, f"영업일 저장 실패: {e}")


@router.delete("/{year}")
async def delete_business_days(year: int):
    """특정 연도의 영업일 설정 삭제"""
    logger.info("[API] 영업일 삭제 | year=%d", year)
    try:
        return await business_calendar_service.delete_business_days(year)
    except Exception as e:
        logger.error("[API] 영업일 삭제 실패 | error=%s", e)
        raise HTTPException(500, f"영업일 삭제 실패: {e}")


