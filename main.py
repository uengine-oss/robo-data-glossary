"""ROBO Data Glossary 서비스

비즈니스 용어집 + 영업일 캘린더 관리 서비스.
robo-data-analyzer에서 분리된 독립 마이크로서비스.

시작 방법:
    uvicorn main:app --host 0.0.0.0 --port 5504 --reload
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.glossary_router import router as glossary_router
from api.business_calendar_router import router as calendar_router
from config.settings import settings
from util.logger import setup_logging, get_logger


# 로깅 초기화
setup_logging()
logger = get_logger(__name__)


# =============================================================================
# FastAPI 앱 설정
# =============================================================================

app = FastAPI(
    title="ROBO Data Glossary",
    description="비즈니스 용어집 및 영업일 캘린더 관리 서비스",
    version=settings.version,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS 미들웨어
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(glossary_router)
app.include_router(calendar_router)


# =============================================================================
# 예외 핸들러
# =============================================================================

@app.exception_handler(RuntimeError)
async def runtime_error_handler(request: Request, exc: RuntimeError):
    logger.error("RuntimeError 발생: %s", str(exc))
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc), "error_type": "RuntimeError"},
    )


# =============================================================================
# 헬스체크
# =============================================================================

@app.get("/")
async def health_check():
    return {"status": "ok", "service": "robo-data-glossary", "version": settings.version}


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "robo-data-glossary", "version": settings.version}


# =============================================================================
# 서버 시작
# =============================================================================

if __name__ == "__main__":
    import uvicorn

    port = int(__import__("os").getenv("PORT", "5504"))
    logger.info("ROBO Data Glossary starting on %s:%d", settings.host, port)
    uvicorn.run("main:app", host=settings.host, port=port, reload=False)
