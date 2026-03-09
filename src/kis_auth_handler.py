"""
Handle auth for KIS
"""

import json
import os
import time
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
        self._token_path = CURRENT_DIR / "access_token.json"
        self.__APP_KEY = os.getenv("KIS_APP_KEY")
        self.__APP_SECRET = os.getenv("KIS_APP_SECRET")
        self._token_cache: dict | None = None
        self._has_logged_token_reuse = False

    @property
    def app_key(self) -> str | None:
        return self.__APP_KEY

    @property
    def app_secret(self) -> str | None:
        return self.__APP_SECRET

    def issue_token(self):
        print("▶ 토큰을 새로 발급합니다.")
        headers = {"Content-Type": "application/json; charset=utf-8"}
        body = {
            "grant_type": "client_credentials",
            "appkey": self.__APP_KEY,
            "appsecret": self.__APP_SECRET,
        }

        res = httpx.post(URL_TOKEN, headers=headers, json=body, timeout=30)
        data = res.json()

        if "access_token" in data:
            # 현재 시간 기준으로 만료 시각 계산
            data["issued_at"] = time.time()
            data["expires_at"] = data["issued_at"] + data["expires_in"]
            with open(self._token_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            self._token_cache = data
            self._has_logged_token_reuse = False
            logger.success("✅ 새 토큰 저장 완료.")
            return data["access_token"]
        else:
            raise Exception(f"토큰 발급 실패: {data}")

    def get_valid_token(self):
        now = time.time()

        # 메모리에 저장된 토큰이 있다면 재사용
        if self._token_cache and now < self._token_cache.get("expires_at", 0):
            if not self._has_logged_token_reuse:
                logger.info("▶ 기존 토큰 사용 (메모리 캐시).")
                self._has_logged_token_reuse = True
            return self._token_cache["access_token"]

        token_data = None
        if os.path.exists(self._token_path):
            with open(self._token_path, "r", encoding="utf-8") as f:
                token_data = json.load(f)

        if token_data and now < token_data.get("expires_at", 0):
            self._token_cache = token_data
            logger.info("▶ 기존 토큰 사용 (파일 로드).")
            self._has_logged_token_reuse = True
            return token_data["access_token"]

        if token_data:
            logger.warning("⚠️ 토큰 만료, 재발급합니다.")
        else:
            logger.warning("⚠️ 토큰 파일 없음, 새로 발급합니다.")

        return self.issue_token()
