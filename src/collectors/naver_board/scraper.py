import re
import httpx
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
from loguru import logger
from playwright.async_api import async_playwright

class NaverBoardScraper:
    BASE_URL = "https://finance.naver.com"
    BOARD_PATH = "/item/board.naver"
    
    def __init__(self, timeout=10):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": self.BASE_URL
        }
        self.timeout = timeout

    async def fetch_list_httpx(self, symbol: str, page: int = 1) -> str | None:
        """Plan A: httpx를 사용한 빠른 정적 크롤링 (EUC-KR 처리 포함)"""
        url = f"{self.BASE_URL}{self.BOARD_PATH}?code={symbol}&page={page}"
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=self.timeout) as client:
                response = await client.get(url)
                response.raise_for_status()
                # Naver Finance는 최신 서비스(Npay 증권 등)에서 UTF-8을 사용하므로 
                # httpx의 자동 인코딩 감지 기능을 활용
                return response.text
        except Exception as e:
            logger.error(f"HTTPX fetch failed for {symbol} page {page}: {e}")
            return None

    async def fetch_list_playwright(self, symbol: str, page: int = 1) -> str | None:
        """Plan B: Playwright를 사용한 브라우저 기반 크롤링 (차단 시 대비)"""
        url = f"{self.BASE_URL}{self.BOARD_PATH}?code={symbol}&page={page}"
        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                ctx = await browser.new_context(user_agent=self.headers["User-Agent"])
                browser_page = await ctx.new_page()
                await browser_page.goto(url, wait_until="domcontentloaded")
                content = await browser_page.content()
                await browser.close()
                return content
        except Exception as e:
            logger.error(f"Playwright fetch failed for {symbol} page {page}: {e}")
            return None

    def parse_list(self, html: str, symbol: str) -> tuple[str, list[dict]]:
        """HTML 파싱하여 종목명과 게시물 목록 추출"""
        soup = BeautifulSoup(html, "html.parser")
        
        # 종목명 추출 (헤더 영역)
        company_name = "Unknown"
        wrap_company = soup.select_one("div.wrap_company h2 a")
        if wrap_company:
            company_name = wrap_company.get_text(strip=True)
        else:
            # 대체 경로
            h2 = soup.select_one("#content > div.section.inner_sub > h2")
            if h2:
                company_name = h2.get_text(strip=True).replace("종목토론실", "").strip()

        posts = []
        # 테이블 행 추출
        table = soup.select_one("table.type2")
        if not table:
            return company_name, []

        rows = table.select("tbody tr")
        for row in rows:
            # td 개수가 4개 미만인 행(간격 조정용)은 건너뜀
            tds = row.select("td")
            if len(tds) < 6:
                continue

            # 날짜 (첫 번째 td)
            date_raw = tds[0].get_text(strip=True)
            if not re.match(r"^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}$", date_raw):
                continue

            # 제목 및 nid 추출 (두 번째 td)
            title_a = tds[1].select_one("a")
            if not title_a:
                continue
            
            title = title_a.get_text(strip=True)
            href = title_a.get("href", "")
            
            # URL에서 nid 추출
            nid = 0
            query = parse_qs(urlparse(str(href)).query)
            if "nid" in query:
                nid = int(query["nid"][0])
            
            if not nid:
                continue

            # 작성자 (세 번째 td)
            author = tds[2].get_text(strip=True)

            # 조회수 (네 번째 td)
            views_raw = tds[3].get_text(strip=True)
            views = int(re.sub(r"[^\d]", "", views_raw)) if re.search(r"\d", views_raw) else 0

            # 공감/비공감 (다섯, 여섯 번째 td)
            likes_raw = tds[4].get_text(strip=True)
            likes = int(re.sub(r"[^\d]", "", likes_raw)) if re.search(r"\d", likes_raw) else 0
            
            dislikes_raw = tds[5].get_text(strip=True)
            dislikes = int(re.sub(r"[^\d]", "", dislikes_raw)) if re.search(r"\d", dislikes_raw) else 0

            posts.append({
                "nid": nid,
                "symbol": symbol,
                "company_name": company_name,
                "date": date_raw,
                "title": title,
                "author": author,
                "views": views,
                "likes": likes,
                "dislikes": dislikes,
                "url": urljoin(self.BASE_URL, href)
            })

        return company_name, posts
