import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import os
import re
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from core.kis_market_handler import MarketHandler
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

app = App(token=os.environ.get("SLACK_BOT_TOKEN"))
market_handler = MarketHandler()

# 전역 에러 핸들러
@app.error
def global_error_handler(error, body, logger):
    logger.exception(f"Error occurred: {error}")

def log_execution(func):
    """함수 실행 시작과 끝을 로깅하는 데코레이터"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        logger.info(f"▶ Starting execution: {func_name}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"✔ Successfully finished: {func_name}")
            return result
        except Exception as e:
            logger.error(f"✖ Failed in {func_name}: {e}")
            raise e
    return wrapper

# 봇 멘션 핸들러
@app.event("app_mention")
@log_execution
def handle_mention(event, say, logger):
    say("네! 호출 확인되었습니다. `!help`라고 입력해 보세요.")

# 명령어 핸들러
@app.message(re.compile(r"^!help$"))
@log_execution
def handle_help(message, say, logger):
    msg = "🤖 *Trading Bot Help*\n\n"
    msg += "• `!buy <종목명> <수량>` : 지정한 종목을 시장가로 매수합니다.\n"
    msg += "• `!sell <종목명> <수량>` : 지정한 종목을 시장가로 매도합니다.\n"
    msg += "• `!balance` : 현재 계좌 잔고 및 보유 종목을 조회합니다.\n"
    msg += "• `!help` : 이 도움말을 표시합니다.\n"
    msg += "• `!status` : 데이터 수집 현황을 확인합니다.\n"
    msg += "• `!backfill <회사명>` : 해당 종목의 과거 1분봉(최근 30일)을 백필합니다."
    say(msg)
 
@app.message(re.compile(r"^!backfill\s+(.+)$"))
@log_execution
def handle_backfill_message(message, say, context, logger):
    try:
        stock_name = context['matches'][0].strip()
        code = market_handler.get_code(stock_name)
        
        if not code:
            say(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
            return
            
        say(f"♻️ {stock_name}({code})의 과거 30일 1분봉 백필을 시작합니다. (이미 수집된 날짜는 건너뜁니다)")
        
        from backfill.intraday_backfill_stock import backfill_stock_intraday
        # Run async function in background or wait
        import asyncio
        results = asyncio.run(backfill_stock_intraday(code, days=30))
        
        msg = f"📊 *{stock_name} 백필 결과*\n"
        msg += f"• 성공: {results['success']}일\n"
        msg += f"• 건너뜀 (이미 존재): {results['skipped']}일\n"
        if results['failed'] > 0:
            msg += f"• 실패: {results['failed']}일\n"
        
        if results['success'] > 0:
            msg += f"• 수집된 날짜: {', '.join(results['dates'][-5:])}"
            if len(results['dates']) > 5: msg += " 등"
            
        say(msg)
        
    except Exception as e:
        logger.error(f"Error in backfill command: {e}")
        say("❌ 백필 처리 중 오류가 발생했습니다.")
 
@app.message("!status")
@log_execution
def handle_status_message(message, say, logger):
    """주요 데이터 카테고리의 마지막 수집 시점 확인"""
    from storage.parquet_writer import _PATHS
    import glob
    
    msg = "🔍 *데이터 수집 현황*\n\n"
    
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
            
        # Recursive search for .parquet files
        files = glob.glob(str(base_dir / "**" / "*.parquet"), recursive=True)
        if not files:
            msg += f"• {label}: ❌ 수집된 데이터 없음\n"
            continue
            
        # Find the most recently modified file (by OS mtime) or by filename date
        latest_file = Path(max(files, key=os.path.getmtime))
        mtime = datetime.fromtimestamp(latest_file.stat().st_mtime)
        
        # Also extract date from filename if possible (YYYY-MM-DD.parquet)
        basename = latest_file.stem
        
        msg += f"• {label}: ✅ 최신 수집 ({basename})\n"
        msg += f"  └ 마지막 배치: _{mtime.strftime('%Y-%m-%d %H:%M')}_\n"
 
    say(msg)

@app.message(re.compile(r"^!buy\s+(.+)\s+(\d+)$"))
@log_execution
def handle_buy_message(message, say, context, logger):
    try:
        stock_name = context['matches'][0].strip()
        quantity = int(context['matches'][1])
        
        code = market_handler.get_code(stock_name)
        if not code:
            say(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
            return
            
        res = market_handler.order_domestic_stock(code=code, quantity=quantity, side="buy", order_type="03")
        
        if res.get("rt_cd") == "0":
            say(f"✅ 매수 주문 성공: {stock_name}({code}) {quantity}주")
        else:
            say(f"❌ 매수 주문 실패: {res.get('msg1')}")
            
    except Exception as e:
        logger.error(f"Error in buy command: {e}")
        say("❌ 주문 처리 중 오류가 발생했습니다.")

@app.message(re.compile(r"^!sell\s+(.+)\s+(\d+)$"))
@log_execution
def handle_sell_message(message, say, context, logger):
    try:
        stock_name = context['matches'][0].strip()
        quantity = int(context['matches'][1])
        
        code = market_handler.get_code(stock_name)
        if not code:
            say(f"❌ '{stock_name}'에 해당하는 종목 코드를 찾을 수 없습니다.")
            return
            
        res = market_handler.order_domestic_stock(code=code, quantity=quantity, side="sell", order_type="03")
        
        if res.get("rt_cd") == "0":
            say(f"✅ 매도 주문 성공: {stock_name}({code}) {quantity}주")
        else:
            say(f"❌ 매도 주문 실패: {res.get('msg1')}")
            
    except Exception as e:
        logger.error(f"Error in sell command: {e}")
        say("❌ 주문 처리 중 오류가 발생했습니다.")

@app.message("!balance")
@log_execution
def handle_balance_message(message, say, logger):
    try:
        balance = market_handler.get_balance()
        # rt_cd가 '0'이거나, rt_cd가 없더라도 output2가 있으면 성공으로 간주
        is_success = balance.get("rt_cd") == "0" or (balance.get("output2") and len(balance.get("output2", [])) > 0)
        
        if is_success:
            output = balance.get("output1", [])
            output2_list = balance.get("output2", [{}])
            output2 = output2_list[0] if output2_list else {}
            
            msg = "📊 *현재 잔고 현황*\n"
            msg += f"• 예수금: {int(output2.get('dnca_tot_amt', 0)):,}원\n"
            msg += f"• 총 평가금액: {int(output2.get('tot_evlu_amt', 0)):,}원\n\n"
            
            if output:
                msg += "*보유 종목:*\n"
                for item in output:
                    name = item.get("prdt_name")
                    qty = int(item.get("hldg_qty", 0))
                    profit_rate = item.get("evlu_pfls_rt", "0")
                    msg += f"• {name}: {qty}주 (수익률: {profit_rate}%)\n"
            else:
                msg += "보유 중인 종목이 없습니다."
                
            say(msg)
        else:
            logger.error(f"Balance inquiry failed: {balance}")
            error_msg = balance.get("msg1") or balance.get("error_description") or "알 수 없는 오류"
            say(f"❌ 잔고 조회 실패: {error_msg}")
    except Exception as e:
        logger.error(f"Error in balance command: {e}")
        say("❌ 잔고 조회 중 오류가 발생했습니다.")


if __name__ == "__main__":
    # 시작 시 권한 체크 테스트
    try:
        auth_test = app.client.auth_test()
        logger.info(f"Bot connected as: {auth_test['user']} (ID: {auth_test['user_id']})")
    except Exception as e:
        logger.error(f"Auth test failed. Check your SLACK_BOT_TOKEN: {e}")

    handler = SocketModeHandler(app, os.environ.get("SLACK_APP_TOKEN"))
    handler.start()
