import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import asyncio
import os
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path

import asyncpg
from loguru import logger
from core.kis_market_handler import MarketHandler

class DataCollector:
    def __init__(self, db_path: str = "trading_data.db"):
        self.postgres_url = self._normalize_postgres_url(os.getenv("TRADING_DATABASE_URL", ""))
        self.backend = "postgres" if self.postgres_url else "sqlite"
        self._pg_pool: asyncpg.Pool | None = None
        raw_db_path = Path(db_path)
        self.db_path = raw_db_path if raw_db_path.is_absolute() else Path(__file__).parents[2] / raw_db_path
        if self.backend == "sqlite":
            self._init_sqlite_db()
        else:
            logger.info("DataCollector initialized with PostgreSQL backend.")
        self.market_handler = None

    @staticmethod
    def _normalize_postgres_url(url: str) -> str:
        return url.replace("postgresql+asyncpg://", "postgresql://", 1).strip()

    def _get_market_handler(self) -> MarketHandler:
        """KIS 인증이 필요한 시점에만 MarketHandler를 생성한다."""
        if self.market_handler is None:
            self.market_handler = MarketHandler()
        return self.market_handler

    def _init_sqlite_db(self):
        """SQLite 데이터베이스 및 테이블 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # 일별 주가 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_prices (
                symbol TEXT,
                date TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                change REAL,
                market TEXT,
                PRIMARY KEY (symbol, date)
            )
        """)

        # 네이버 종목 토론방 게시물 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_board_posts (
                nid INTEGER PRIMARY KEY,
                symbol TEXT,
                company_name TEXT,
                date TEXT,
                title TEXT,
                author TEXT,
                views INTEGER,
                likes INTEGER,
                dislikes INTEGER,
                url TEXT
            )
        """)
        conn.commit()
        conn.close()
        logger.info(f"SQLite database initialized at {self.db_path}")

    async def _ensure_postgres(self) -> asyncpg.Pool:
        if not self.postgres_url:
            raise RuntimeError("TRADING_DATABASE_URL is not set")
        if self._pg_pool is None:
            self._pg_pool = await asyncpg.create_pool(self.postgres_url, min_size=1, max_size=8)
            await self._init_postgres_db()
            logger.info("PostgreSQL database initialized.")
        return self._pg_pool

    async def _init_postgres_db(self):
        if self._pg_pool is None:
            raise RuntimeError("PostgreSQL pool is not initialized")
        async with self._pg_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS daily_prices (
                    symbol TEXT,
                    date TEXT,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    change DOUBLE PRECISION,
                    market TEXT,
                    PRIMARY KEY (symbol, date)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_board_posts (
                    nid BIGINT PRIMARY KEY,
                    symbol TEXT,
                    company_name TEXT,
                    date TEXT,
                    title TEXT,
                    author TEXT,
                    views INTEGER,
                    likes INTEGER,
                    dislikes INTEGER,
                    url TEXT
                )
            """)

    async def close(self):
        if self._pg_pool is not None:
            await self._pg_pool.close()
            self._pg_pool = None

    def collect_domestic_stock(self, symbol: str, timeframe: str = "D", days: int = 100):
        """국내 주식 데이터 수집 및 저장"""
        logger.info(f"Collecting domestic data for {symbol}...")
        market_handler = self._get_market_handler()
        market_handler.exchange = "서울"
        
        end_day = datetime.now().strftime("%Y%m%d")
        start_day = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        # limit=days 인자 추가 (페이지네이션 작동을 위해)
        data = market_handler.fetch_ohlcv(symbol, timeframe=timeframe, start_day=start_day, end_day=end_day, limit=days)
        
        if not data:
            logger.warning(f"No data found for {symbol}")
            return

        records = []
        for d in data:
            records.append((
                symbol,
                d.get("stck_bsop_date"),
                float(d.get("stck_oprc", 0)),
                float(d.get("stck_hgpr", 0)),
                float(d.get("stck_lwpr", 0)),
                float(d.get("stck_clpr", 0)),
                int(d.get("acml_vol", 0)),
                float(d.get("prdy_vrss", 0)),
                "KRX"
            ))
        
        self._save_to_db(records)

    def collect_oversea_stock(self, symbol: str, exchange: str = "나스닥", timeframe: str = "D", days: int = 100):
        """해외 주식 데이터 수집 및 저장"""
        logger.info(f"Collecting oversea data for {symbol} ({exchange})...")
        market_handler = self._get_market_handler()
        market_handler.exchange = exchange
        
        # 페이지네이션 지원을 위해 limit 전달
        data = market_handler.fetch_ohlcv(symbol, timeframe=timeframe, limit=days)
        
        if not data:
            logger.warning(f"No data found for {symbol}")
            return

        records = []
        for d in data:
            records.append((
                symbol,
                d.get("xymd"),
                float(d.get("open", 0)),
                float(d.get("high", 0)),
                float(d.get("low", 0)),
                float(d.get("clos", 0)),
                int(d.get("tvol", 0)),
                float(d.get("diff", 0)),
                exchange
            ))
        
        self._save_to_db(records)

    def collect_oversea_index(self, symbol: str, timeframe: str = "D", days: int = 100):
        """해외 지수(나스닥 등) 데이터 수집 및 저장"""
        logger.info(f"Collecting oversea index data for {symbol}...")
        market_handler = self._get_market_handler()
        
        # 해외 지수는 전용 메서드 사용
        data = market_handler.fetch_oversea_index_ohlcv(symbol, timeframe=timeframe)
        
        if not data:
            logger.warning(f"No data found for index {symbol}")
            return

        records = []
        for d in data:
            records.append((
                symbol,
                d.get("stck_bsop_date"),
                float(d.get("stck_oprc", 0)),
                float(d.get("stck_hgpr", 0)),
                float(d.get("stck_lwpr", 0)),
                float(d.get("stck_clpr", 0)),
                int(d.get("acml_vol", 0)),
                float(d.get("prdy_vrss", 0)),
                "INDEX"
            ))
        
        self._save_to_db(records)

    def _save_to_db(self, records: list):
        """DB에 대량 삽입 (중복 시 교체)"""
        if self.backend == "postgres":
            asyncio.run(self.save_daily_prices_async(records))
            return

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.executemany("""
                INSERT OR REPLACE INTO daily_prices 
                (symbol, date, open, high, low, close, volume, change, market)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn.commit()
            logger.success(f"Saved {len(records)} records to database.")
        except Exception as e:
            logger.error(f"Failed to save to DB: {e}")
        finally:
            conn.close()

    def save_board_posts(self, records: list):
        """종목 토론방 게시물 DB 저장 (중복 시 무시)"""
        if self.backend == "postgres":
            asyncio.run(self.save_board_posts_async(records))
            return

        if not records:
            return
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.executemany("""
                INSERT OR IGNORE INTO stock_board_posts 
                (nid, symbol, company_name, date, title, author, views, likes, dislikes, url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn.commit()
            logger.success(f"Saved {len(records)} board posts to database.")
        except Exception as e:
            logger.error(f"Failed to save board posts to DB: {e}")
        finally:
            conn.close()

    def get_last_board_nid(self, symbol: str) -> int:
        """특정 종목의 가장 최근 게시물 ID 조회"""
        if self.backend == "postgres":
            return asyncio.run(self.get_last_board_nid_async(symbol))

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT MAX(nid) FROM stock_board_posts WHERE symbol = ?", (symbol,))
            res = cursor.fetchone()
            return res[0] if res and res[0] else 0
        except Exception as e:
            logger.error(f"Failed to get last nid: {e}")
            return 0
        finally:
            conn.close()

    async def save_daily_prices_async(self, records: list):
        """일별 가격 데이터를 PostgreSQL에 저장한다."""
        if not records:
            return
        pool = await self._ensure_postgres()
        async with pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO daily_prices
                (symbol, date, open, high, low, close, volume, change, market)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    change = EXCLUDED.change,
                    market = EXCLUDED.market
            """, records)
        logger.success(f"Saved {len(records)} records to PostgreSQL.")

    async def save_board_posts_async(self, records: list):
        """종목 토론방 게시물을 PostgreSQL에 저장한다."""
        if not records:
            return
        pool = await self._ensure_postgres()
        async with pool.acquire() as conn:
            await conn.executemany("""
                INSERT INTO stock_board_posts
                (nid, symbol, company_name, date, title, author, views, likes, dislikes, url)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (nid) DO NOTHING
            """, records)
        logger.success(f"Saved {len(records)} board posts to PostgreSQL.")

    async def get_last_board_nid_async(self, symbol: str) -> int:
        """특정 종목의 가장 최근 게시물 ID를 PostgreSQL 또는 SQLite에서 조회한다."""
        if self.backend == "postgres":
            pool = await self._ensure_postgres()
            async with pool.acquire() as conn:
                result = await conn.fetchval("SELECT MAX(nid) FROM stock_board_posts WHERE symbol = $1", symbol)
                return int(result) if result else 0

        return self.get_last_board_nid(symbol)

if __name__ == "__main__":
    collector = DataCollector()
    
    # 1. KOSPI 200 핵심 종목 수집 (페이지네이션 테스트 포함)
    kospi_subset = ["005930", "000660", "373220", "207940", "005380"]
    logger.info(f"Starting collection for {len(kospi_subset)} KOSPI stocks...")
    for symbol in kospi_subset:
        collector.collect_domestic_stock(symbol, days=200)
        time.sleep(0.5)
    
    # 2. 해외 주식 수집 (QQQ, TSLA, NVDA)
    oversea_list = ["QQQ", "TSLA", "NVDA"]
    logger.info(f"Starting collection for {len(oversea_list)} Oversea stocks...")
    for symbol in oversea_list:
        collector.collect_oversea_stock(symbol, exchange="나스닥", days=50)
        time.sleep(0.5)

    # 3. 해외 지수 수집
    indices = [".COMP", "NAS@COMP", "SNI@SPX"]
    logger.info(f"Starting collection for {len(indices)} Indices...")
    for symbol in indices:
        collector.collect_oversea_index(symbol, days=50)
        time.sleep(0.5)
