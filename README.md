# ROBO Data Glossary

비즈니스 용어집 및 영업일 캘린더 관리 마이크로서비스.

`robo-data-analyzer`에서 분리된 독립 서비스로, Neo4j 기반 용어 관리와 LLM 기반 용어 자동 추출 기능을 제공합니다.

## 주요 기능

- 용어집 CRUD (생성/조회/수정/삭제)
- 용어 관리 (도메인, 소유자, 태그, 상태)
- LLM 기반 용어 자동 추출 (Excel/CSV 업로드 → AI 분석)
- 영업일 캘린더 관리 (공휴일, 비영업일)

## 아키텍처

```
robo-data-glossary (port 5504)
├── api/                  # FastAPI 라우터
│   ├── glossary_router.py
│   └── business_calendar_router.py
├── service/              # 비즈니스 로직
│   ├── glossary_manage_service.py
│   ├── glossary_bulk_service.py
│   └── business_calendar_service.py
├── client/               # 외부 클라이언트
│   ├── neo4j_client.py
│   └── llm_client.py
├── config/               # 환경 설정
│   └── settings.py
├── util/                 # 유틸리티
│   └── logger.py
└── main.py               # 서비스 진입점
```

## API 엔드포인트

### 용어집

| Method | Path | 설명 |
|--------|------|------|
| GET | `/robo/glossary/` | 용어집 목록 |
| POST | `/robo/glossary/` | 용어집 생성 |
| GET | `/robo/glossary/{id}` | 용어집 상세 |
| PUT | `/robo/glossary/{id}` | 용어집 수정 |
| DELETE | `/robo/glossary/{id}` | 용어집 삭제 |
| GET | `/robo/glossary/{id}/terms` | 용어 목록 |
| POST | `/robo/glossary/{id}/terms` | 용어 생성 |
| POST | `/robo/glossary/{id}/terms/bulk-upload` | 파일 업로드 |
| POST | `/robo/glossary/{id}/terms/bulk-extract` | AI 용어 추출 |
| GET | `/robo/glossary/{id}/terms/bulk-extract/stream` | 추출 스트리밍 |
| POST | `/robo/glossary/{id}/terms/bulk-import` | 용어 일괄 등록 |
| GET | `/robo/glossary/meta/domains` | 도메인 목록 |
| GET | `/robo/glossary/meta/owners` | 소유자 목록 |
| GET | `/robo/glossary/meta/tags` | 태그 목록 |

### 영업일 캘린더

| Method | Path | 설명 |
|--------|------|------|
| GET | `/robo/business-calendar/{year}` | 연도별 캘린더 |
| POST | `/robo/business-calendar/` | 캘린더 설정 |
| GET | `/robo/business-calendar/check/{date}` | 영업일 확인 |
| DELETE | `/robo/business-calendar/{year}` | 캘린더 삭제 |
| GET | `/robo/business-calendar/non-business-dates` | 비영업일 목록 |

## 실행 방법

### 환경 변수 (.env)

```env
NEO4J_URI=bolt://127.0.0.1:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password
LLM_API_KEY=your_api_key
LLM_MODEL=gpt-4.1
```

### 설치 및 실행

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 5504
```

### API 문서

실행 후 http://localhost:5504/docs 에서 Swagger UI 확인 가능.

## Neo4j 스키마

```
(Glossary)-[:HAS_TERM]->(Term)
(Term)-[:BELONGS_TO_DOMAIN]->(Domain)
(Term)-[:OWNED_BY]->(Owner)
(Term)-[:REVIEWED_BY]->(Owner)
(Term)-[:HAS_TAG]->(Tag)
(BusinessCalendar)-[:HAS_NON_BUSINESS_DAY]->(NonBusinessDay)
(BusinessCalendar)-[:HAS_HOLIDAY]->(Holiday)
```
