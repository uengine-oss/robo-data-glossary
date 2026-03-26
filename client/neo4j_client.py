"""Neo4j 비동기 클라이언트 (Glossary 전용)

쿼리 실행 및 결과 반환만 담당하는 경량 클라이언트.
"""

import logging
import warnings
from typing import Any, Optional

from neo4j import AsyncGraphDatabase

from config.settings import settings

warnings.filterwarnings("ignore", category=DeprecationWarning, module="neo4j")
warnings.filterwarnings("ignore", message=".*Received notification from DBMS server.*")
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)


class Neo4jClient:
    __slots__ = ("_driver", "_config", "_database")

    def __init__(self, database: Optional[str] = None):
        self._config = settings.neo4j
        self._database = database if database is not None else self._config.database
        self._driver = AsyncGraphDatabase.driver(
            self._config.uri,
            auth=(self._config.user, self._config.password),
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        if self._driver:
            await self._driver.close()

    async def execute_queries(
        self,
        queries: list[str | dict[str, Any]],
        params: Optional[dict] = None,
    ) -> list[Any]:
        """Cypher 쿼리 실행 및 결과 반환"""
        if not queries:
            return []

        try:
            async with self._driver.session(database=self._database) as session:
                results: list[list[dict[str, Any]]] = []
                for query in queries:
                    if isinstance(query, dict):
                        query_str = str(query.get("query") or "").strip()
                        if not query_str:
                            raise RuntimeError("execute_queries dict item requires non-empty 'query'")
                        item_params = query.get("parameters")
                        if item_params is None:
                            item_params = query.get("params")
                        merged_params = {**(params or {}), **(item_params or {})}
                        query_result = await session.run(query_str, merged_params)
                    else:
                        query_result = await session.run(query, params or {})

                    results.append(await query_result.data())

                return results

        except Exception as e:
            raise RuntimeError(f"Cypher 쿼리 실행 중 오류 발생 (query_count={len(queries)}): {e}") from e
