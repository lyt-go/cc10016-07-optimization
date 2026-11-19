"""跨模块数据链路由：暴露8组以上贯通链路API。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from app.engine import (
    chain_source_seed_parse_field,
    chain_schedule_session_proxy_rate,
    chain_anti_retry_cursor,
    chain_convert_dedup_validate_store,
    chain_extract_convert,
    chain_render_source,
    chain_monitor_schedule,
    chain_full_pipeline,
    chain_seed_cursor_advance,
)
from app.schemas import ActionResult
from app.services.quota import quota_service

router = APIRouter(prefix="/api/chain", tags=["跨模块链路"])


@router.post("/source-seed-parse-field", response_model=ActionResult)
def api_source_seed_parse_field(source_id: int) -> dict[str, Any]:
    """链路1: 采集源 -> 种子 -> 解析规则 -> 字段抽取"""
    result = chain_source_seed_parse_field(source_id)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/schedule-session-proxy-rate", response_model=ActionResult)
def api_schedule_session_proxy_rate(schedule_id: int) -> dict[str, Any]:
    """链路2: 采集编排 -> 会话 -> 代理池 -> 频次控制"""
    result = chain_schedule_session_proxy_rate(schedule_id)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/anti-retry-cursor", response_model=ActionResult)
def api_anti_retry_cursor(source_id: int, response_text: str = "", status_code: int = 200,
                          exc_name: str = "TimeoutError", attempt: int = 1) -> dict[str, Any]:
    """链路3: 反爬对抗 -> 重试策略 -> 增量游标"""
    result = chain_anti_retry_cursor(source_id, response_text, status_code, exc_name, attempt)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/convert-dedup-validate-store", response_model=ActionResult)
def api_convert_dedup_validate_store(dedup_id: int, validate_id: int, convert_id: int,
                                     store_id: int, records: list[dict[str, Any]]) -> dict[str, Any]:
    """链路4: 格式转换 -> 去重 -> 校验 -> 结果落地"""
    result = chain_convert_dedup_validate_store(dedup_id, validate_id, convert_id, store_id, records)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/extract-convert", response_model=ActionResult)
def api_extract_convert(extract_id: int, convert_id: int, html: str) -> dict[str, Any]:
    """链路5: 内容抽取 -> 格式转换"""
    result = chain_extract_convert(extract_id, convert_id, html)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/render-source", response_model=ActionResult)
def api_render_source(source_id: int, resource_url: str) -> dict[str, Any]:
    """链路6: 渲染编排 -> 采集源资源过滤"""
    result = chain_render_source(source_id, resource_url)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/monitor-schedule", response_model=ActionResult)
def api_monitor_schedule(schedule_id: int) -> dict[str, Any]:
    """链路7: 监控 -> 采集编排执行状态"""
    result = chain_monitor_schedule(schedule_id)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/seed-cursor-advance", response_model=ActionResult)
def api_seed_cursor_advance(seed_id: int, new_position: str) -> dict[str, Any]:
    """链路8: 种子 -> 增量游标推进"""
    result = chain_seed_cursor_advance(seed_id, new_position)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.post("/full-pipeline", response_model=ActionResult)
def api_full_pipeline(source_id: int, html: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    """链路9: 全链路贯通：源配置 -> 抽取 -> 转换 -> 去重 -> 校验 -> 落地"""
    result = chain_full_pipeline(source_id, html, records)
    return {"ok": result.get("ok", False), "message": result.get("message", ""), "entry": result}


@router.get("/quota-consistency/{rate_limit_id}")
def api_quota_consistency(rate_limit_id: int) -> dict[str, Any]:
    """核对配额状态：令牌数 + 在途令牌 = 并发上限，当日用量与流水一致。"""
    result = quota_service.check(rate_limit_id)
    return {"ok": result["ok"], "message": "配额状态一致" if result["ok"] else "、".join(result["issues"]),
            "entry": result}


@router.get("/quota-policy")
def api_quota_policy() -> dict[str, Any]:
    """配额状态数据的统一口径与写入顺序说明。"""
    return {"ok": True, "policy": quota_service.policy()}
