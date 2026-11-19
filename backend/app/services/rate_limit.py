"""频次控制业务规则：令牌桶限流、并发计数、日配额追踪。

配额状态数据统一由 :mod:`app.services.quota` 加工，本模块只负责策略本身与
调用配额服务，保证刷新后的令牌数、当日用量与请求日志数来自同一份状态。
"""
from __future__ import annotations

from typing import Any

from app.services.quota import quota_service
from app.store import store

MODULE = "rate_limit"
LIST_FIELDS = ["策略编号", "目标源", "请求间隔", "并发上限", "日请求总量", "冷却时长", "队列上限", "优先级"]
REQUIRED_FIELDS = ["策略编号", "目标源"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
NEGATIVE_ACTIONS = ["停用策略"]


class RateLimitService:
    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("策略编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        for row in rows[start:start + size]:
            d = dict(row)
            d.update(self._quota_fields(int(d["id"])))
            enriched.append(d)
        return enriched, total

    def _quota_fields(self, entry_id: int) -> dict[str, Any]:
        """生成配额状态数据：配额对象、界面描述与原始状态一次给全。"""
        state = quota_service.ensure(entry_id)
        return {
            "_quota": state,
            "_quota_state": quota_service.refresh_view(entry_id),
            "_tokens": state["tokens"],
            "_daily_used": state["daily_used"],
            "_capacity": state["capacity"],
            "_request_log_count": state["request_log_count"],
            "_quota_version": state["version"],
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d.update(self._quota_fields(entry_id))
        return d

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        # 绑定采集源
        source_id = values.get("关联采集源ID")
        if source_id:
            try:
                sid = int(source_id)
                store.add_relation("source_to_rate_limit", "crawl_source", sid, MODULE, entry["id"])
            except (ValueError, TypeError):
                pass
        # 生成配额状态（令牌桶按并发上限初始化，当日用量归零）
        quota_service.ensure(entry["id"])
        entry.update(quota_service.refresh_view(entry["id"]))
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"限流策略 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于频次控制可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"限流策略已{action}"

    def acquire_token(
        self,
        entry_id: int,
        *,
        request_log_id: str | None = None,
        session_id: int | None = None,
        order: str = "quota_first",
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """尝试获取一个请求令牌；令牌数与当日用量在同一次提交里扣减。"""
        return quota_service.acquire(
            entry_id, request_log_id=request_log_id, session_id=session_id,
            order=order, expected_version=expected_version)

    def release_token(
        self,
        entry_id: int,
        *,
        token_id: str | None = None,
        expected_version: int | None = None,
        reconcile_usage: bool = True,
    ) -> dict[str, Any]:
        """释放令牌回桶；重复释放幂等，令牌不会超过并发上限。"""
        return quota_service.release(
            entry_id, token_id=token_id, expected_version=expected_version,
            reconcile_usage=reconcile_usage)

    def refresh_quota(self, entry_id: int, *, expected_version: int | None = None) -> dict[str, Any]:
        """读取采集请求配额并刷新令牌状态。"""
        result = quota_service.refresh(entry_id, expected_version=expected_version)
        if entry_id is not None and store.find(MODULE, entry_id) is not None:
            result["quota_state"] = quota_service.refresh_view(entry_id)
        return result

