"""会话管理业务规则：会话超时轮转、Cookie摘要刷新、请求计数追踪。

会话在请求日志写入前后各核对一次配额状态：日志落库后立刻确认对应令牌，
扣减失败的请求保留日志并在释放前后补记用量，避免刷新后配额状态比实际请求宽松。
"""
from __future__ import annotations

from typing import Any

from app.services.quota import (
    CHECK_KEY,
    DEFAULT_WRITE_ORDER,
    ORDER_LOG_FIRST,
    ORDER_QUOTA_FIRST,
    WRITE_ORDERS,
    now_iso,
    quota_service,
)
from app.store import store

MODULE = "session_mgr"
LIST_FIELDS = ["会话编号", "关联源", "登录状态", "最后活动", "请求计数", "Cookie摘要", "UserAgent", "会话时长"]
REQUIRED_FIELDS = ["会话编号", "关联源"]
STATUS_ORDER = ["草稿", "活跃", "已过期", "已失效"]
ACTION_RULES = {"标记活跃": "活跃", "标记过期": "已过期", "标记失效": "已失效"}
NEGATIVE_ACTIONS = ["标记失效"]


def _check_session_expiry(entry: dict[str, Any]) -> bool:
    """检查会话是否超过时长限制（模拟）。"""
    try:
        duration = int(str(entry.get("会话时长") or "3600").replace("s", "").replace("秒", ""))
    except (ValueError, TypeError):
        duration = 3600
    req_count = int(str(entry.get("请求计数") or "0"))
    # 模拟：请求数超过阈值或固定超时即过期
    return req_count > duration / 10


class SessionMgrService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("会话编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        enriched = []
        logs = store.pipeline_state("session_request_logs") or {}
        checks = store.pipeline_state(CHECK_KEY) or {}
        for row in rows[start:start + size]:
            d = dict(row)
            expired = _check_session_expiry(d)
            d["_expired"] = expired
            d["_request_log"] = logs.get(str(d["id"]), [])
            d["_quota_checks"] = checks.get(str(d["id"]), [])[-5:]
            d["_quota_state"] = self._quota_view(d)
            enriched.append(d)
        return enriched, total

    def _quota_view(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """会话页展示的配额摘要：仅当关联源绑定了限流策略时生成。"""
        rate_id = self.resolve_rate_limit(entry)
        if rate_id is None:
            return None
        state = quota_service.ensure(rate_id)
        return {
            "rate_limit_id": rate_id,
            "令牌数": state["tokens"],
            "当日已用配额": state["daily_used"],
            "请求日志数": state["request_log_count"],
            "数据版本": state["version"],
        }

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        d = dict(entry)
        d["_expired"] = _check_session_expiry(d)
        logs = store.pipeline_state("session_request_logs") or {}
        d["_request_log"] = logs.get(str(entry_id), [])
        checks = store.pipeline_state(CHECK_KEY) or {}
        d["_quota_checks"] = checks.get(str(entry_id), [])[-5:]
        d["_quota_state"] = self._quota_view(d)
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
        entry["请求计数"] = "0"
        rows.append(entry)
        # 绑定代理
        proxy_id = values.get("绑定代理ID")
        if proxy_id:
            try:
                pid = int(proxy_id)
                store.add_relation("session_to_proxy", MODULE, entry["id"], "proxy_pool", pid)
            except (ValueError, TypeError):
                pass
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"采集会话 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于会话管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"采集会话已{action}"

    def _source_id(self, entry: dict[str, Any]) -> int | None:
        """会话关联源可能是源标识（SRC-ELEC-001）或源ID，统一解析为源ID。"""
        raw = str(entry.get("关联源") or "").strip()
        if not raw:
            return None
        try:
            return int(raw)
        except (ValueError, TypeError):
            pass
        for source in store.rows("crawl_source"):
            if str(source.get("源标识") or "") == raw:
                return int(source["id"])
        return None

    def resolve_rate_limit(self, entry: dict[str, Any]) -> int | None:
        """找到会话关联源绑定的限流策略；跨模块联动以关系索引为准。"""
        source_id = self._source_id(entry)
        if source_id is None:
            return None
        rels = store.get_relations("source_to_rate_limit", source_module="crawl_source", source_id=source_id)
        if not rels:
            return None
        return int(rels[-1]["target_id"])

    def _log_checks(self, session_id: int, check: dict[str, Any]) -> None:
        checks = store.pipeline_state(CHECK_KEY)
        if not isinstance(checks, dict):
            checks = {}
        history = checks.setdefault(str(session_id), [])
        history.append(check)
        if len(history) > 100:
            checks[str(session_id)] = history[-100:]
        store.set_pipeline_state(CHECK_KEY, checks)

    def record_request(
        self,
        entry_id: int,
        url: str,
        latency_ms: float,
        *,
        order: str = DEFAULT_WRITE_ORDER,
        rate_limit_id: int | None = None,
    ) -> dict[str, Any]:
        """记录会话请求并核对配额：写日志前后各取一次令牌，二者必须对应同一个令牌。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "会话不存在"}
        order = order if order in WRITE_ORDERS else DEFAULT_WRITE_ORDER
        try:
            count = int(str(entry.get("请求计数") or "0")) + 1
        except (ValueError, TypeError):
            count = 1

        rate_id = rate_limit_id if rate_limit_id is not None else self.resolve_rate_limit(entry)
        before = quota_service.snapshot(rate_id) if rate_id else None
        request_log_id = f"REQ-{entry_id}-{count}"
        quota_result: dict[str, Any] = {"linked": False, "reason": "会话未绑定限流策略"}

        if rate_id is not None:
            if order == ORDER_LOG_FIRST:
                quota_result = self._record_log_first(rate_id, entry_id, request_log_id, before)
            else:
                quota_result = self._record_quota_first(rate_id, entry_id, request_log_id, before)
            # 请求日志此刻已经写入，再取一次配额状态核对自己获得的令牌。
            quota_result["after"] = quota_service.snapshot(rate_id)
            quota_result["state"] = quota_service.ensure(rate_id)
            quota_result["before"] = before
            quota_result["consistent"] = self._checks_out(quota_result)
            quota_result["arbitration_decision"] = (
                "同一配额对象只保留一份状态，版本递增后可重新核对")

        entry["请求计数"] = str(count)
        entry["最后活动"] = "2026-11-16T00:00:00Z"
        logs = store.pipeline_state("session_request_logs") or {}
        log = logs.setdefault(str(entry_id), [])
        log.append({
            "url": url,
            "latency": latency_ms,
            "count": count,
            "log_id": request_log_id,
            "rate_limit_id": rate_id,
            "quota_ok": bool(quota_result.get("allowed", quota_result.get("linked", False))),
        })
        if len(log) > 100:
            logs[str(entry_id)] = log[-100:]
        store.set_pipeline_state("session_request_logs", logs)

        check = {
            "log_id": request_log_id,
            "rate_limit_id": rate_id,
            "order": order,
            "at": now_iso(),
            "order_ok": bool(quota_result.get("allowed", True)),
            "quota": quota_result,
        }
        self._log_checks(entry_id, check)
        return {
            "ok": True,
            "request_count": count,
            "request_log_id": request_log_id,
            "rate_limit_id": rate_id,
            "order": order,
            "quota": quota_result,
        }

    def _checks_out(self, result: dict[str, Any]) -> bool:
        """写日志前后核对：本次请求要么绑定到唯一令牌，要么显式进入待补记。"""
        after = result.get("after") or {}
        if result.get("pending_usage"):
            return int(after.get("request_log_count", 0)) >= 1
        token_id = result.get("token_id")
        if not token_id:
            return False
        reservation = (result.get("state") or {}).get("outstanding", {}).get(str(token_id)) or {}
        return reservation.get("log_id") in (None, result.get("request_log_id"))

    def _record_log_first(
        self, rate_id: int, session_id: int, request_log_id: str, before: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """先记录请求：日志已经完整保留，扣减失败时补记用量。"""
        expected = int(before["version"]) if before else None
        result = quota_service.acquire(
            rate_id, request_log_id=request_log_id, session_id=session_id,
            order=ORDER_LOG_FIRST, expected_version=expected)
        result["linked"] = bool(result.get("allowed"))
        if result.get("allowed"):
            return result
        quota_service.mark_log_pending(rate_id, request_log_id, session_id=session_id)
        result["pending_usage"] = True
        result["request_log_count"] = quota_service.snapshot(rate_id)["request_log_count"]
        result["usable_relation"] = "请求日志数、令牌数与当日用量在释放时补齐后互相核对"
        return result

    def _record_quota_first(
        self, rate_id: int, session_id: int, request_log_id: str, before: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """先扣配额：限流立即生效，请求日志晚到后把日志编号补挂到配额对象上。"""
        expected = int(before["version"]) if before else None
        result = quota_service.acquire(
            rate_id, session_id=session_id, order=ORDER_QUOTA_FIRST, expected_version=expected)
        if not result.get("allowed"):
            # 配额未放行：调用日志仍然保留，并进入待补记用量，由释放前后补齐。
            quota_service.mark_log_pending(rate_id, request_log_id, session_id=session_id)
            result["linked"] = False
            result["pending_log"] = True
            result["pending_usage"] = True
            result["request_log_count"] = quota_service.snapshot(rate_id)["request_log_count"]
            return result
        attach = quota_service.attach_request_log(rate_id, str(result["token_id"]), request_log_id)
        state = attach.get("state") or quota_service.ensure(rate_id)
        result["request_log_id"] = request_log_id
        result["linked"] = bool(attach.get("ok"))
        result["state"] = state
        result["request_log_count"] = state.get("request_log_count")
        result["quota_first_note"] = "配额状态先显示新余量，随后附上对应请求日志编号"
        return result

    def release_request(
        self,
        entry_id: int,
        *,
        request_log_id: str | None = None,
        token_id: str | None = None,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """按请求日志释放令牌；释放前后单独补齐用量写入，重复释放幂等。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return {"ok": False, "error": "会话不存在"}
        checks = store.pipeline_state(CHECK_KEY) or {}
        history = checks.get(str(entry_id)) or []
        record = next((item for item in reversed(history) if item.get("log_id") == request_log_id), None) \
            if request_log_id else (history[-1] if history else None)
        quota_result = (record or {}).get("quota") or {}
        rate_id = (record or {}).get("rate_limit_id") or self.resolve_rate_limit(entry)
        if rate_id is None:
            return {"ok": False, "error": "会话未绑定限流策略"}
        if token_id is None and request_log_id:
            # 按请求日志找到它当初拿到的令牌，保证释放和扣减对应同一个配额对象。
            links = store.pipeline_state("quota_consumption_links") or {}
            token_id = (links.get(str(request_log_id)) or {}).get("token_id")
            if token_id is None and not quota_result.get("pending_usage"):
                token_id = quota_service.snapshot(int(rate_id)).get("pending_token")
        result = quota_service.release(
            int(rate_id), token_id=token_id, request_log_id=request_log_id,
            expected_version=expected_version)
        if result.get("ok"):
            # release() 内部已经补记过一次；重复释放时这里不会再补，统一沿用结果里的计数。
            usage = {"completed": int(result.get("usage_completed", 0))}
            if usage["completed"] == 0:
                usage["completed"] = quota_service.reconcile_pending_usage(int(rate_id)).get("completed", 0)
        else:
            usage = {"completed": 0}
        latest = quota_service.snapshot(int(rate_id))
        return {
            **result,
            "rate_limit_id": int(rate_id),
            "usage_completed": usage.get("completed", 0),
            "quota": latest,
            "consistent": int(latest["tokens"]) + int(latest["in_flight"]) == int(latest["capacity"]),
        }

    def rotate_session(self, source_id: int) -> dict[str, Any] | None:
        """为指定采集源轮替到一个活跃会话。"""
        rels = store.get_relations("schedule_to_session", target_module=MODULE)
        for rel in rels:
            sess = store.find(MODULE, rel["target_id"])
            if sess and sess.get("关联源") == str(source_id) and sess.get("status") == "活跃":
                return dict(sess)
        # 查找该源下任意活跃会话
        for row in store.rows(MODULE):
            if row.get("关联源") == str(source_id) and row.get("status") == "活跃":
                return dict(row)
        return None
