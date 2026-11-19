"""频次控制接口：维护限流策略，覆盖提交校验、标记生效、停用策略等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.rate_limit import RateLimitService

router = APIRouter(prefix="/api/rate_limit", tags=["频次控制"])

service = RateLimitService()

LIST_FIELDS = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
STATUSES = ["草稿", "待校验", "已生效", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按策略编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已生效、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、策略编号关键字与状态过滤频次控制列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status", "令牌数", "当日已用配额", "请求日志数", "数据版本"])
    for item in items:
        quota = item.get("_quota_state") or {}
        row = []
        for value in [
            item.get("id"),
            *(item.get(field, "") for field in LIST_FIELDS),
            item.get("status", ""),
            quota.get("令牌数", ""),
            quota.get("当日已用配额", ""),
            quota.get("请求日志数", ""),
            quota.get("数据版本", ""),
        ]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="rate_limit.csv"'})


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条限流策略明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"限流策略 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条限流策略，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="限流策略已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条限流策略执行提交校验、标记生效、停用策略；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/acquire")
def acquire_token(entry_id: int, payload: dict[str, object] | None = None) -> dict:
    """尝试获取请求令牌；可带请求日志编号与写入顺序（quota_first / log_first）。"""
    payload = payload or {}
    request_log_id = payload.get("request_log_id")
    expected_version = payload.get("expected_version")
    return service.acquire_token(
        entry_id,
        request_log_id=str(request_log_id) if request_log_id else None,
        session_id=int(payload["session_id"]) if payload.get("session_id") is not None else None,
        order=str(payload.get("order") or "quota_first"),
        expected_version=int(expected_version) if expected_version is not None else None,
    )


@router.post("/{entry_id}/release")
def release_token(entry_id: int, payload: dict[str, object] | None = None) -> dict:
    """释放令牌回桶；重复释放同一 token_id 幂等。"""
    payload = payload or {}
    expected_version = payload.get("expected_version")
    return service.release_token(
        entry_id,
        token_id=str(payload["token_id"]) if payload.get("token_id") else None,
        expected_version=int(expected_version) if expected_version is not None else None,
        reconcile_usage=bool(payload.get("reconcile_usage", True)),
    )


@router.post("/{entry_id}/refresh")
def refresh_quota(entry_id: int, payload: dict[str, object] | None = None) -> dict:
    """读取采集请求配额并刷新令牌状态，返回刷新后的配额状态数据。"""
    payload = payload or {}
    expected_version = payload.get("expected_version")
    return service.refresh_quota(
        entry_id,
        expected_version=int(expected_version) if expected_version is not None else None,
    )


@router.get("/{entry_id}/quota")
def get_quota(entry_id: int) -> dict:
    """只读配额状态数据：令牌数、当日已用配额与请求日志数。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"限流策略 {entry_id} 不存在或已归档")
    return {"entry_id": entry_id, "quota": entry["_quota"], "quota_state": entry["_quota_state"]}
