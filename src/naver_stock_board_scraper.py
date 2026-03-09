from debugpy.common.timestamp import current
import re
from dataclasses import dataclass
from playwright.async_api import async_playwright
from urllib.parse import urljoin, urlparse, parse_qs
import pandas as pd
from pathlib import Path
import arrow


PAGINATION_SEL = "#content > div.section.inner_sub > table.tbl_pagination"
NEXT_SEL = f"{PAGINATION_SEL} table.Nnavi td.pgR a"  # '다음' 링크
CUR_PAGE_SEL = f"{PAGINATION_SEL} table.Nnavi td.on a"  # 현재 페이지 번호 셀
TBODY_SEL = "#content > div.section.inner_sub > table.type2 > tbody"
BASE = "https://finance.naver.com"


@dataclass
class StockBoardPost:
    date: str
    title: str
    views: int | None
    url: str | None


async def start_browser():
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(
        headless=False,
        slow_mo=50
    )
    context = await browser.new_context(
        viewport={"width": 1280, "height": 2000}
    )
    page = await context.new_page()
    return playwright, browser, context, page


async def close_browser(playwright, browser):

    await browser.close()
    await playwright.stop()


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


async def extract_rows_from_tbody(page):
    tbody = page.locator(TBODY_SEL)
    await tbody.wait_for(state="visible")

    trs = tbody.locator("tr")
    n = await trs.count()

    out = []
    for i in range(n):
        tr = trs.nth(i)

        # td 개수로 유효 row만 필터링 (헤더 th, 공백/클린봇/구분선 등 제거)
        tds = tr.locator("td")
        td_cnt = await tds.count()
        if td_cnt < 4:
            continue

        # date: 첫번째 td의 span
        date = _clean(await tds.nth(0).inner_text())
        # date 형식 아니면 skip
        if not re.match(r"^\d{4}\.\d{2}\.\d{2}\s+\d{2}:\d{2}$", date):
            continue

        # title + href: 두번째 td.title 안의 a
        title_td = tds.nth(1)
        a = title_td.locator("a").first
        if await a.count() == 0:
            continue

        title = _clean(await a.inner_text())
        href = await a.get_attribute("href")
        url = urljoin(BASE, href) if href else None

        # views: 네번째 td (index 3)
        views_raw = _clean(await tds.nth(3).inner_text())
        # 숫자만 남김
        views = int(re.sub(r"[^\d]", "", views_raw)) if re.search(r"\d", views_raw) else None

        out.append({
            "date": date,
            "title": title,
            "views": views,
            "url": url,
        })

    return out


async def _get_next_href(page) -> str | None:
    nav = page.locator(PAGINATION_SEL)
    if await nav.count() == 0:
        return None

    nxt = page.locator(NEXT_SEL).first
    if await nxt.count() == 0:
        return None

    href = await nxt.get_attribute("href")
    return href


async def update_last_page_no(page, current: int | None) -> int | None:
    next_href = await _get_next_href(page)
    if not next_href:
        return None
    query = parse_qs(urlparse(next_href).query)
    next_pg_num = int(query.get("page", [0])[0])
    if current is not None and next_pg_num <= current:
        return None
    return next_pg_num


async def get_current_page_no(page) -> int | None:
    cur = page.locator(CUR_PAGE_SEL).first
    if await cur.count() == 0:
        return None
    txt = (await cur.inner_text()).strip()
    return int(txt) if txt.isdigit() else None


async def main(code: str, max_pages: int = 200):
    url = f"{BASE}/item/board.naver?code={code}"
    print(f"Starting crawl for {code} at {url}")
    last_pg_num = 1
    current_pg_num = 1
    output: list[StockBoardPost] = []
    try:
        playwright, browser, context, page = await start_browser()
        await page.goto(url, wait_until="domcontentloaded")
        await page.wait_for_selector("tbody > tr > th")

        found_last_pg_num = await get_current_page_no(page)
        last_pg_num = found_last_pg_num or last_pg_num

        while current_pg_num <= last_pg_num and current_pg_num <= max_pages:
            rows = await extract_rows_from_tbody(page)
            for row in rows:
                post_data = StockBoardPost(**row)
                output.append(post_data)
            last_pg_num = await update_last_page_no(page, last_pg_num) or last_pg_num
            print(f"Extracted page {current_pg_num - 1}, total posts so far: {len(output)}")
            current_pg_num += 1
            if current_pg_num == last_pg_num:
                found_last_pg_num = await get_current_page_no(page)
                if found_last_pg_num and found_last_pg_num > last_pg_num:
                    last_pg_num = found_last_pg_num
                    print(f"Updated last page number to {last_pg_num} based on current page info.")
            await page.goto(url + f"&page={current_pg_num}", wait_until="domcontentloaded")
            await page.wait_for_selector("tbody > tr > th")

    except Exception as e:
        print(f"Error during crawling: {e}")
    finally:
        await close_browser(playwright, browser)

    if not output:
        print(f"No posts extracted for {code}.")
        return

    df = pd.DataFrame([post.__dict__ for post in output])
    outdir = Path("data")
    outdir.mkdir(exist_ok=True)
    outpath = outdir / f"{code}_stock_board_posts_{arrow.now().strftime('%Y-%m-%d')}.parquet"
    df.to_parquet(outpath, index=False)
    print(f"Saved {len(df)} posts to {outpath}")


if __name__ == "__main__":
    import fire
    fire.Fire(main)
