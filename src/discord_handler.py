import os
import discord
from discord.ext import commands
from kis_market_handler import MarketHandler
from loguru import logger
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import yaml
from functools import wraps

CURRENT_DIR = Path(__file__).resolve().parent

# Load environment variables
KEY_PATH = Path.home() / ".ssh" / "kis"
load_dotenv(KEY_PATH)

# Load config from config.yaml if it exists
CONFIG_PATH = CURRENT_DIR.parent / "config.yaml"
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
        for k, v in config.items():
            os.environ[k] = str(v)

# Discord Bot Setup
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
market_handler = MarketHandler()

def log_execution(func):
    """함수 실행 시작과 끝을 로깅하는 데코레이터"""
    @wraps(func)
    async def wrapper(ctx, *args, **kwargs):
        func_name = func.__name__
        logger.info(f"▶ Starting execution: {func_name}")
        try:
            result = await func(ctx, *args, **kwargs)
            logger.info(f"✔ Successfully finished: {func_name}")
            return result
        except Exception as e:
            logger.error(f"✖ Failed in {func_name}: {e}")
            await ctx.send(f"❌ 오류가 발생했습니다: {e}")
    return wrapper

@bot.event
async def on_ready():
    logger.info(f"Bot connected as: {bot.user} (ID: {bot.user_id})")

@bot.command(name="help_trading") # 'help' is built-in, so we use a different name or override
async def help_trading(ctx):
    msg = "🤖 **Trading Bot Help**\n\n"
    msg += "• `!buy <종목명> <수량>` : 지정한 종목을 시장가로 매수합니다.\n"
    msg += "• `!sell <종목명> <수량>` : 지정한 종목을 시장가로 매도합니다.\n"
    msg += "• `!balance` : 현재 계좌 잔고 및 보유 종목을 조회합니다.\n"
    msg += "• `!status` : 데이터 수집 현황을 확인합니다.\n"
    msg += "• `!backfill <회사명>` : 해당 종목의 과거 1분봉(최근 30일)을 백필합니다."
    await ctx.send(msg)

@bot.command(name="buy")
@log_execution
async def buy(ctx, stock_name: str, quantity: int):
    code = market_handler.get_code(stock_name)
    if not code:
        await ctx.send(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
        return

    res = market_handler.order_domestic_stock(code=code, quantity=quantity, side="buy", order_type="03")

    if res.get("rt_cd") == "0":
        await ctx.send(f"✅ 매수 주문 성공: {stock_name}({code}) {quantity}주")
    else:
        await ctx.send(f"❌ 매수 주문 실패: {res.get('msg1')}")

@bot.command(name="sell")
@log_execution
async def sell(ctx, stock_name: str, quantity: int):
    code = market_handler.get_code(stock_name)
    if not code:
        await ctx.send(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
        return

    res = market_handler.order_domestic_stock(code=code, quantity=quantity, side="sell", order_type="03")

    if res.get("rt_cd") == "0":
        await ctx.send(f"✅ 매도 주문 성공: {stock_name}({code}) {quantity}주")
    else:
        await ctx.send(f"❌ 매도 주문 실패: {res.get('msg1')}")

@bot.command(name="balance")
@log_execution
async def balance(ctx):
    balance_res = market_handler.get_balance()
    is_success = balance_res.get("rt_cd") == "0" or (balance_res.get("output2") and len(balance_res.get("output2", [])) > 0)

    if is_success:
        output = balance_res.get("output1", [])
        output2_list = balance_res.get("output2", [{}])
        output2 = output2_list[0] if output2_list else {}

        msg = "📊 **현재 잔고 현황**\n"
        msg += f"• 예수금: {int(output2.get('dnca_tot_amt', 0)):,}원\n"
        msg += f"• 총 평가금액: {int(output2.get('tot_evlu_amt', 0)):,}원\n\n"

        if output:
            msg += "**보유 종목:**\n"
            for item in output:
                name = item.get("prdt_name")
                qty = int(item.get("hldg_qty", 0))
                profit_rate = item.get("evlu_pfls_rt", "0")
                msg += f"• {name}: {qty}주 (수익률: {profit_rate}%)\n"
        else:
            msg += "보유 중인 종목이 없습니다."

        await ctx.send(msg)
    else:
        error_msg = balance_res.get("msg1") or balance_res.get("error_description") or "알 수 없는 오류"
        await ctx.send(f"❌ 잔고 조회 실패: {error_msg}")

@bot.command(name="status")
@log_execution
async def status(ctx):
    from parquet_writer import _PATHS
    import glob

    msg = "🔍 **데이터 수집 현황**\n\n"
    categories = {
        "KOSPI 200 (1분)": "kospi200_1min",
        "KOSPI 200 (성분)": "kospi200_components",
        "투자자 매매동향": "investor_flow_daily",
        "NASDAQ (1분)": "nasdaq_1min",
        "국내 주식 (일봉)": "kr_stock_daily"
    }

    for label, key in categories.items():
        base_dir = _PATHS.get(key)
        if not base_dir or not base_dir.exists():
            msg += f"• {label}: ❌ 데이터 경로 없음\n"
            continue

        files = glob.glob(str(base_dir / "**" / "*.parquet"), recursive=True)
        if not files:
            msg += f"• {label}: ❌ 수집된 데이터 없음\n"
            continue

        latest_file = Path(max(files, key=os.path.getmtime))
        mtime = datetime.fromtimestamp(latest_file.stat().st_mtime)
        basename = latest_file.stem

        msg += f"• {label}: ✅ 최신 수집 ({basename})\n"
        msg += f"  └ 마지막 배치: _{mtime.strftime('%Y-%m-%d %H:%M')}_\n"

    await ctx.send(msg)

@bot.command(name="backfill")
@log_execution
async def backfill(ctx, stock_name: str):
    code = market_handler.get_code(stock_name)
    if not code:
        await ctx.send(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
        return

    await ctx.send(f"♻️ {stock_name}({code})의 과거 30일 1분봉 백필을 시작합니다... (중복 제외)")

    from intraday_backfill_stock import backfill_stock_intraday
    results = await backfill_stock_intraday(code, days=30)

    msg = f"📊 **{stock_name} 백필 결과**\n"
    msg += f"• 성공: {results['success']}일\n"
    msg += f"• 건너뜀 (이미 존재): {results['skipped']}일\n"
    if results['failed'] > 0:
        msg += f"• 실패: {results['failed']}일\n"

    if results['success'] > 0:
        msg += f"• 수집된 날짜: {', '.join(results['dates'][-5:])}"
        if len(results['dates']) > 5: msg += " 등"

    await ctx.send(msg)

if __name__ == "__main__":
    token = os.environ.get("DISCORD_TOKEN", "89aa27de4507f60dc5920e0d4f91a83ef46d24881edebdbb721391d7ae87226b")
    if not token:
        logger.error("DISCORD_TOKEN not found in environment variables.")
    else:
        bot.run(token)
