# 📊 Codex CLI 기반 Daily 뉴스 요약 자동화 (Prefect Workflow)

## 1. 🎯 목표
- 매일 오전 07:00 자동 실행
- 뉴스 수집 → 요약 → Obsidian Markdown 저장
- 추가 API 비용 없이 운영

## 2. 🏗️ 아키텍처
Prefect Scheduler → 뉴스 수집 → 전처리 → Codex CLI → Obsidian 저장

## 3. 📁 프로젝트 구조
news_pipeline/
├── flows/
├── tasks/
├── prompts/
├── config/
└── obsidian/

## 4. ⚙️ Prefect Flow
- fetch_news
- preprocess_news
- run_codex

## 5. 🧠 Codex 실행 핵심
codex --approval-mode full-auto

## 6. ⏰ 스케줄
0 7 * * *

## 7. 🧾 결과
Daily/YYYY-MM-DD.md 생성

## 8. 🚀 확장
뉴스 → 요약 → sentiment → signal → trading

