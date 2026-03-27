"""
Handle auth for KIS
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path

import httpx
from dotenv import load_dotenv
from loguru import logger

from kis_config import URL_TOKEN

KEY_PATH = Path.home() / ".ssh" / "kis"
load_dotenv(KEY_PATH)
CURRENT_DIR = Path(__file__).parent


class KISAuthHandler:
    def __init__(self):
        # Load environment variables from .ssh/kis
        KEY_PATH = Path.home() / ".ssh" / "kis"
        load_dotenv(KEY_PATH)

        self.app_key = os.getenv("KIS_APP_KEY")
        self.app_secret = os.getenv("KIS_APP_SECRET")
        self.is_simulation = os.getenv("KIS_SIMULATION", "false").lower() == "true"
        
        if self.is_simulation:
            self.base_url = "https://openapivts.koreainvestment.com:29443"
        else:
            self.base_url = "https://openapi.koreainvestment.com:9443"

        if not self.app_key or not self.app_secret:
            raise ValueError("KIS_APP_KEY and KIS_APP_SECRET must be set in ~/.ssh/kis")

        self.token_file = CURRENT_DIR.parent / "access_token.json"
        self._access_token = None

    def _save_token(self, token_data: dict):
        """토큰 정보와 서버에서 준 만료 시간을 함께 저장"""
        expired_at_str = token_data.get("access_token_token_expired")
        if expired_at_str:
            dt = datetime.strptime(expired_at_str, "%Y-%m-%d %H:%M:%S")
            token_data["expired_at_timestamp"] = dt.timestamp()
            token_data["app_key"] = self.app_key

        with open(self.token_file, "w", encoding="utf-8") as f:
            json.dump(token_data, f, ensure_ascii=False, indent=2)

    def _load_token(self) -> dict | None:
        """파일에서 토큰 정보를 로드"""
        if not self.token_file.exists():
            return None
        try:
            with open(self.token_file, "r", encoding="utf-8") as f:
                return json.load(f)
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
