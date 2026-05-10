"""
Handle auth for KIS
"""

import base64
import json
import os
import time
from datetime import datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv
from loguru import logger

CURRENT_DIR = Path(__file__).parent


class KISAuthHandler:
    def __init__(self):
        key_path = Path.home() / ".ssh" / "kis"
        if key_path.exists():
            load_dotenv(key_path)
        load_dotenv()

        self.credential_profile = self._resolve_profile()
        self.app_key, self.app_secret = self._resolve_api_credentials(self.credential_profile)
        self.account_number, self.account_product_code = self._resolve_account_credentials(
            self.credential_profile
        )
        self.is_simulation = self.credential_profile == "paper" or (
            os.getenv("KIS_SIMULATION", "false").lower() == "true"
        )

        if self.is_simulation:
            self.base_url = "https://openapivts.koreainvestment.com:29443"
        else:
            self.base_url = "https://openapi.koreainvestment.com:9443"

        if not self.app_key or not self.app_secret:
            raise ValueError("KIS API credentials are missing. Set KIS_* or PAPER_* in ~/.ssh/kis.")

        self.token_file = CURRENT_DIR.parent / f"access_token.{self.credential_profile}.json"
        self._access_token = None
        logger.info(
            "Initialized KIS auth profile={} simulation={} account_configured={}",
            self.credential_profile,
            self.is_simulation,
            bool(self.account_number),
        )

    def _decode_jwt_exp(self, access_token: str | None) -> int | None:
        """JWT payload의 exp 클레임을 읽어 Unix timestamp로 반환"""
        if not access_token:
            return None

        try:
            parts = access_token.split(".")
            if len(parts) < 2:
                return None

            payload = parts[1]
            payload += "=" * (-len(payload) % 4)
            decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
            return int(json.loads(decoded).get("exp"))
        except Exception:
            return None

    def _normalize_expired_at_timestamp(self, token_data: dict) -> bool:
        """토큰 만료 시각을 환경과 무관한 값으로 정규화"""
        jwt_exp = self._decode_jwt_exp(token_data.get("access_token"))
        if jwt_exp:
            changed = token_data.get("expired_at_timestamp") != float(jwt_exp)
            token_data["expired_at_timestamp"] = float(jwt_exp)
            return changed

        expired_at_str = token_data.get("access_token_token_expired")
        if expired_at_str:
            dt = datetime.strptime(expired_at_str, "%Y-%m-%d %H:%M:%S")
            normalized = dt.timestamp()
            changed = token_data.get("expired_at_timestamp") != normalized
            token_data["expired_at_timestamp"] = normalized
            return changed

        return False

    def _resolve_profile(self) -> str:
        requested = (os.getenv("KIS_PROFILE") or "").strip().lower()
        has_live = bool(os.getenv("KIS_APP_KEY") and os.getenv("KIS_APP_SECRET"))
        has_paper = bool(os.getenv("PAPER_APP_KEY") and os.getenv("PAPER_APP_SECRET"))

        if requested in {"paper", "mock", "simulation"}:
            return "paper"
        if requested in {"live", "real", "production"}:
            return "live"
        if has_paper:
            return "paper"
        if has_live:
            return "live"
        return "live"

    def _resolve_api_credentials(self, profile: str) -> tuple[str | None, str | None]:
        if profile == "paper":
            return os.getenv("PAPER_APP_KEY"), os.getenv("PAPER_APP_SECRET")
        return os.getenv("KIS_APP_KEY"), os.getenv("KIS_APP_SECRET")

    def _resolve_account_credentials(self, profile: str) -> tuple[str, str]:
        if profile == "paper":
            account = (
                os.getenv("PAPER_ACCOUNT")
                or os.getenv("PAPER_CANO")
                or os.getenv("PAPER_KIS_CANO")
                or ""
            )
            product_code = (
                os.getenv("PAPER_ACNT_PRDT_CD")
                or os.getenv("PAPER_ACCOUNT_PRODUCT_CODE")
                or os.getenv("KIS_ACNT_PRDT_CD")
                or "01"
            )
            return account, product_code

        account = os.getenv("KIS_CANO") or os.getenv("KIS_ACCOUNT") or ""
        product_code = os.getenv("KIS_ACNT_PRDT_CD", "01")
        return account, product_code

    def _save_token(self, token_data: dict):
        """토큰 정보와 서버에서 준 만료 시간을 함께 저장"""
        self._normalize_expired_at_timestamp(token_data)
        token_data["app_key"] = self.app_key

        with open(self.token_file, "w", encoding="utf-8") as f:
            json.dump(token_data, f, ensure_ascii=False, indent=2)

    def _load_token(self) -> dict | None:
        """파일에서 토큰 정보를 로드"""
        if not self.token_file.exists():
            return None
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                token_data = json.load(f)
            if isinstance(token_data, dict):
                changed = self._normalize_expired_at_timestamp(token_data)
                if token_data.get("app_key") != self.app_key:
                    token_data["app_key"] = self.app_key
                    changed = True
                if changed:
                    self._save_token(token_data)
            return token_data
        except Exception:
            return None

    def _is_token_valid(self, token_data: dict | None) -> bool:
        """토큰의 유효성 검사 (키 일치 여부 및 만료 시간)"""
        if not token_data or "access_token" not in token_data:
            return False
        
        if token_data.get("app_key") != self.app_key:
            return False

        expired_at = token_data.get("expired_at_timestamp")
        if expired_at:
            # 1분 마진을 두고 체크
            return time.time() < (expired_at - 60)
        
        return False

    def get_valid_token(self) -> str:
        """유효한 토큰 반환"""
        token_data = self._load_token()
        
        if self._is_token_valid(token_data):
            # token_data가 None이 아님을 _is_token_valid에서 보장함
            return token_data["access_token"]  # type: ignore

        return self._issue_new_token()

    def force_refresh_token(self) -> str:
        """캐시를 무시하고 새 토큰 발급"""
        return self._issue_new_token()

    def _issue_new_token(self) -> str:
        """새로운 접근 토큰 발급"""
        logger.info("▶ Issuing new KIS access token...")
        url = f"{self.base_url}/oauth2/tokenP"
        headers = {"content-type": "application/json"}
        data = {
            "grant_type": "client_credentials",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }

        resp = httpx.post(url, headers=headers, json=data, timeout=10)
        resp.raise_for_status()
        token_data = resp.json()

        if "access_token" in token_data:
            self._save_token(token_data)
            return token_data["access_token"]
        else:
            raise RuntimeError(f"Failed to issue token: {token_data}")
