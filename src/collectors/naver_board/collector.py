import asyncio
from datetime import datetime
from loguru import logger
import fire
import sys
from pathlib import Path

# 프로젝트 루트(src)를 path에 추가하여 절대 임포트 가능하게 함
current_dir = Path(__file__).resolve().parent
src_dir = current_dir.parents[1]
if src_dir not in sys.path:
    sys.path.append(str(src_dir))

# 이제 src를 기준으로 임포트 (src 아래에 있으므로 절대 경로 지원)
try:
    from collectors.naver_board.scraper import NaverBoardScraper
    from data_collector import DataCollector
    from notifier import Notifier
except ImportError:
    # 로컬 실행 시 예외 처리
    from scraper import NaverBoardScraper
    from data_collector import DataCollector
    from notifier import Notifier

class NaverBoardCollector:
    def __init__(self, db_path: str = "trading_data.db"):
        self.scraper = NaverBoardScraper()
        self.db = DataCollector(db_path=db_path)
        self.notifier = Notifier()

    async def collect_symbol(self, symbol: str, max_pages: int = 10):
        """특정 종목의 게시물을 증분 수집"""
        logger.info(f"[{symbol}] Collection started (max_pages={max_pages})")
        
        last_nid = self.db.get_last_board_nid(symbol)
        logger.info(f"[{symbol}] Last collected nid in DB: {last_nid}")
        
        all_new_posts = []
        company_name = "Unknown"
        
        for page in range(1, max_pages + 1):
            logger.info(f"[{symbol}] Scraping page {page}...")
            
            # Plan A: HTTPX
            html = await self.scraper.fetch_list_httpx(symbol, page)
            if not html:
                logger.warning(f"[{symbol}] Plan A failed, trying Plan B (Playwright)...")
                html = await self.scraper.fetch_list_playwright(symbol, page)
            
            if not html:
                logger.error(f"[{symbol}] Failed to fetch page {page} with both plans.")
                break
            
            curr_company_name, posts = self.scraper.parse_list(html, symbol)
            if curr_company_name != "Unknown":
                company_name = curr_company_name
            
            if not posts:
                logger.warning(f"[{symbol}] No posts found on page {page}.")
                break
            
            # 증분 수집 로직: DB에 있는 nid를 만나면 중단
            new_posts_in_page = []
            reached_last_nid = False
            for p in posts:
                if p["nid"] <= last_nid:
                    reached_last_nid = True
                    break
                new_posts_in_page.append(p)
            
            all_new_posts.extend(new_posts_in_page)
            logger.info(f"[{symbol}] Page {page}: Found {len(new_posts_in_page)} new posts.")
            
            if reached_last_nid:
                logger.info(f"[{symbol}] Reached last collected nid. Stopping.")
                break
            
            # 과도한 요청 방지
            await asyncio.sleep(0.5)

        if all_new_posts:
            # DB 저장용 레코드 변환 (nid, symbol, company_name, date, title, author, views, likes, dislikes, url)
            records = [
                (p["nid"], p["symbol"], p["company_name"], p["date"], p["title"], 
                 p["author"], p["views"], p["likes"], p["dislikes"], p["url"])
                for p in all_new_posts
            ]
            self.db.save_board_posts(records)
            logger.success(f"[{symbol}] Successfully saved {len(all_new_posts)} new posts.")
        else:
            logger.info(f"[{symbol}] No new posts to save.")
            
        # 가장 반응 좋은 게시물(좋아요 기준) 추출
        best_post = None
        if all_new_posts:
            best_post = max(all_new_posts, key=lambda x: x["likes"])
            
        return {
            "symbol": symbol,
            "company_name": company_name,
            "count": len(all_new_posts),
            "best_post": best_post
        }

    async def run(self, symbols: str = "000660,005930", max_pages: int = 5):
        """여러 종목에 대해 병렬 수집 실행"""
        symbol_list = [s.strip() for s in symbols.split(",")]
        logger.info(f"Starting parallel collection for: {symbol_list}")
        
        tasks = [self.collect_symbol(s, max_pages) for s in symbol_list]
        results = await asyncio.gather(*tasks)
        
        # 알림 요약 생성
        total_new = sum(r["count"] for r in results)
        if total_new > 0:
            msg = "📊 *네이버 종목 토론방 수집 리포트*\n"
            msg += f"• 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            msg += f"• 총 신규 게시물: {total_new}건\n\n"
            
            for r in results:
                msg += f"✅ *{r['company_name']}*({r['symbol']}): {r['count']}건\n"
                if r["best_post"] and r["best_post"]["likes"] > 0:
                    best = r["best_post"]
                    msg += f"  └ 🔥 *인기글*: {best['title'][:30]}... (👍{best['likes']})\n"
            
            await self.notifier.notify_all(msg)
            logger.info("Sent summary report to Slack/Discord.")
        else:
            logger.info("No new posts collected. Skipping notification.")

def main(symbols: str = "000660,005930", max_pages: int = 5):
    collector = NaverBoardCollector()
    asyncio.run(collector.run(symbols, max_pages))

if __name__ == "__main__":
    fire.Fire(main)
