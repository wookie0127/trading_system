import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from loguru import logger
from kis_market_handler import MarketHandler

class DataCollector:
    def __init__(self, db_path: str = "trading_data.db"):
        self.db_path = Path(__file__).parent.parent / db_path
        self._init_db()
        self.market_handler = MarketHandler()

    def _init_db(self):
        """데이터베이스 및 테이블 초기화"""
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
        logger.info(f"Database initialized at {self.db_path}")

    def collect_domestic_stock(self, symbol: str, timeframe: str = "D", days: int = 100):
        """국내 주식 데이터 수집 및 저장"""
        logger.info(f"Collecting domestic data for {symbol}...")
        self.market_handler.exchange = "서울"
        
        end_day = datetime.now().strftime("%Y%m%d")
        start_day = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        
        # limit=days 인자 추가 (페이지네이션 작동을 위해)
        data = self.market_handler.fetch_ohlcv(symbol, timeframe=timeframe, start_day=start_day, end_day=end_day, limit=days)
        
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
        self.market_handler.exchange = exchange
        
        # 페이지네이션 지원을 위해 limit 전달
        data = self.market_handler.fetch_ohlcv(symbol, timeframe=timeframe, limit=days)
        
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
        
        # 해외 지수는 전용 메서드 사용
        data = self.market_handler.fetch_oversea_index_ohlcv(symbol, timeframe=timeframe)
        
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
