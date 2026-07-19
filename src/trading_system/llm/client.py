import os
from typing import Optional, Tuple
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

try:
    from google import genai
    from google.genai import types

    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

from src.trading_system.llm.schemas import TradingDecision, Evidence
from src.trading_system.llm.prompts import SYSTEM_INSTRUCTION, build_user_prompt
from src.trading_system.snapshots.schemas import MarketSnapshot


class GeminiDecisionClient:
    def __init__(
        self, model_name: str = "gemini-2.5-flash", api_key: Optional[str] = None
    ):
        self.model_name = model_name
        self.api_key = api_key or os.getenv("GEMINI_API_KEY")
        if HAS_GENAI and self.api_key:
            self.client = genai.Client(api_key=self.api_key)
        else:
            self.client = None

    def get_decision(
        self, snapshot: MarketSnapshot, strategy_guidelines: str = ""
    ) -> Tuple[TradingDecision, str, bool]:
        """
        Calls Gemini API with structured output schema.
        Returns: (TradingDecision, raw_response_str, is_success)
        If any failure occurs, returns safe Fail-Closed NO_TRADE decision.
        """
        user_prompt = build_user_prompt(snapshot, strategy_guidelines)

        if not self.client:
            logger.warning(
                "Gemini Client not initialized (missing key or library). Returning Fail-closed NO_TRADE."
            )
            return (
                self._fallback_no_trade("Gemini client not initialized"),
                "CLIENT_NOT_INITIALIZED",
                False,
            )

        try:
            raw_text = self._call_gemini_api_with_retry(user_prompt)
            parsed_decision = TradingDecision.model_validate_json(raw_text)
            return parsed_decision, raw_text, True

        except Exception as e:
            logger.error(f"Gemini API call or validation failed after retries: {e}")
            return self._fallback_no_trade(f"Error: {e}"), str(e), False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def _call_gemini_api_with_retry(self, user_prompt: str) -> str:
        """
        Internal helper with Tenacity retry backoff for API robustness.
        """
        config = types.GenerateContentConfig(
            system_instruction=SYSTEM_INSTRUCTION,
            temperature=0.2,
            top_p=0.95,
            response_mime_type="application/json",
            response_schema=TradingDecision,
        )
        response = self.client.models.generate_content(
            model=self.model_name,
            contents=user_prompt,
            config=config,
        )
        return response.text or ""

    def _fallback_no_trade(self, reason: str) -> TradingDecision:
        return TradingDecision(
            regime="UNCERTAIN",
            strategy="NONE",
            action="NO_TRADE",
            confidence=0.0,
            thesis=f"Fallback NO_TRADE due to system error: {reason}",
            supporting_evidence=[],
            contradicting_evidence=[Evidence(category="technical", description=reason)],
            invalidation_conditions=["API error resolved"],
            alternative_scenario="Stay flat",
            risk_flags=["FAIL_CLOSED_TRIGGERED"],
            risk_profile="CONSERVATIVE",
        )
