"""配额状态数据：频次控制与会话管理的统一口径。

处理路径（沿用频次控制既有实现再向下延伸）：

    HTTP 路由
      ├─ rate_limit.list/get ──┐
      ├─ rate_limit.acquire ───┤
      ├─ rate_limit.release ───┤──> QuotaStateService（唯一写入方）
      ├─ rate_limit.refresh ───┤        │
      └─ session_mgr.record ───┘        ├─ rate_limit_states   配额状态缓存（一份）
                                        ├─ rate_limit_ledger   已落库的扣减/释放/回滚流水
                                        └─ quota_consumption_links  请求日志编号关联

统一口径（trade-off 取舍）：

* ``tokens``       并发桶余量 = 并发上限 - 在途令牌，释放后最多回到并发上限。
* ``daily_used``   当日已用配额 = 当日已扣减数 + 已补记的请求用量，释放不回收。
* ``in_flight``    已获取未释放的令牌数，恒满足 ``tokens + in_flight == capacity``。
* ``request_log_count`` 与本策略关联过的请求日志数；日志可能早于或晚于扣减出现。
* ``pending_log_count`` 尚待补齐的日志/用量数（未挂日志的在途令牌 + 待补记用量日志）。

落库流水（ledger）是唯一事实来源；``rate_limit_states`` 只是可重建的缓存。每次提交
都先把流水写下去，再由流水重放生成缓存，因此缓存不可能只更新令牌数或只更新当日用量。

两种写入顺序由调用方显式选择：

* ``quota_first``：先扣配额，限流立即生效；请求日志稍后写入，日志编号补挂到配额对象。
* ``log_first``：先写完整调用日志再扣配额；扣减失败时日志保留，用量在释放前后补记。

版本冲突仲裁：每次写入递增 ``version``。调用方带 ``expected_version`` 时，若版本已
变化则按优先级仲裁——扣减(3) > 释放/挂日志(2) > 刷新(1)。扣减基于最新版本重试，
低优先级操作让位并返回冲突，避免一次迟到的释放放宽下一次限流判断。
"""
from __future__ import annotations

import copy
import threading
from datetime import datetime, timezone
from typing import Any

from app.store import store

STATE_KEY = "rate_limit_states"
LEDGER_KEY = "rate_limit_ledger"
LINK_KEY = "quota_consumption_links"
CHECK_KEY = "session_quota_checks"

OP_PRIORITY = {"refresh": 1, "release": 2, "attach_log": 2, "usage": 2, "acquire": 3}
ORDER_QUOTA_FIRST = "quota_first"
ORDER_LOG_FIRST = "log_first"
WRITE_ORDERS = (ORDER_QUOTA_FIRST, ORDER_LOG_FIRST)
DEFAULT_WRITE_ORDER = ORDER_LOG_FIRST

# 状态缓存与落库流水不一致时的回滚原因，便于界面与日志核对。
ROLLBACK_TOKEN_OVERFLOW = "令牌数超过并发上限"
ROLLBACK_DAILY_LAG = "当日用量落后于已落库扣减"
ROLLBACK_DAILY_AHEAD = "当日用量超出已落库扣减"
ROLLBACK_TOKENS_MISMATCH = "令牌余量与在途令牌不匹配"
ROLLBACK_LOG_MISMATCH = "请求日志数与配额状态不一致"
ROLLBACK_DAY_ROLLOVER = "跨日配额重置"
ROLLBACK_DUPLICATE_STATE = "同一策略存在多份配额状态"

_LOCK = threading.RLock()


def today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_number(value: Any, default: int) -> int:
    try:
        return int(str(value).replace("ms", "").replace("s", "").replace("次", ""))
    except (ValueError, TypeError):
        return default


class QuotaStateService:
    """配额状态数据的唯一读写入口。所有方法在锁内完成读-改-写。"""

    # ------------------------------------------------------------------
    # 基础存取
    # ------------------------------------------------------------------
    def _states(self) -> dict[str, Any]:
        states = store.pipeline_state(STATE_KEY)
        if not isinstance(states, dict):
            states = {}
            store.set_pipeline_state(STATE_KEY, states)
        return states

    def _ledger(self) -> list[dict[str, Any]]:
        ledger = store.pipeline_state(LEDGER_KEY)
        if not isinstance(ledger, list):
            ledger = []
            store.set_pipeline_state(LEDGER_KEY, ledger)
        return ledger

    def _links(self) -> dict[str, Any]:
        links = store.pipeline_state(LINK_KEY)
        if not isinstance(links, dict):
            links = {}
            store.set_pipeline_state(LINK_KEY, links)
        return links

    def capacity(self, entry_id: int) -> tuple[int, int]:
        entry = store.find("rate_limit", entry_id) or {}
        return (
            max(0, parse_number(entry.get("并发上限"), 10)),
            max(0, parse_number(entry.get("日请求总量"), 1000)),
        )

    def _new_state(self, entry_id: int, version: int = 0) -> dict[str, Any]:
        capacity, daily_capacity = self.capacity(entry_id)
        return {
            "entry_id": entry_id,
            "version": version,
            "day": today(),
            "tokens": capacity,
            "capacity": capacity,
            "daily_used": 0,
            "daily_capacity": daily_capacity,
            "issued_count": 0,
            "released_count": 0,
            "usage_count": 0,
            "in_flight": 0,
            "request_log_count": 0,
            "pending_log_count": 0,
            "outstanding": {},
            "last_op": "init",
            "last_op_priority": 0,
            "updated_at": now_iso(),
        }

    # ------------------------------------------------------------------
    # 落库流水重放：配额状态缓存的唯一生成方式
    # ------------------------------------------------------------------
    def _events_for_day(self, entry_id: int, day: str) -> list[dict[str, Any]]:
        return [
            event for event in self._ledger()
            if event.get("entry_id") == entry_id and event.get("day") == day
        ]

    def _pending_usage_logs(self, entry_id: int, day: str) -> list[str]:
        """已保留调用日志、但当日用量尚未补记的日志编号。"""
        pending: list[str] = []
        used: set[str] = set()
        for event in self._events_for_day(entry_id, day):
            log_id = event.get("log_id")
            if not log_id:
                continue
            if event.get("op") == "log_pending":
                pending.append(str(log_id))
            elif event.get("op") == "usage":
                used.add(str(log_id))
        return [log_id for log_id in dict.fromkeys(pending) if log_id not in used]

    def _rebuild(self, entry_id: int) -> dict[str, Any]:
        """仅依赖落库流水重建当日配额状态。"""
        state = self._new_state(entry_id)
        day = state["day"]
        events = self._events_for_day(entry_id, day)

        outstanding: dict[str, dict[str, Any]] = {}
        issued = 0
        released = 0
        log_ids: set[str] = set()
        usage_logs: set[str] = set()

        for event in events:
            op = event.get("op")
            token_id = event.get("token_id")
            log_id = event.get("log_id")
            if op == "acquire":
                issued += 1
                outstanding[str(token_id)] = {
                    "token_id": token_id,
                    "log_id": log_id,
                    "session_id": event.get("session_id"),
                    "order": event.get("order"),
                    "at": event.get("at"),
                }
            elif op == "attach_log":
                reservation = outstanding.get(str(token_id))
                if reservation is not None:
                    reservation["log_id"] = log_id
            elif op == "release":
                if str(token_id) not in outstanding:
                    continue  # 重复释放不重复计数
                outstanding.pop(str(token_id), None)
                released += 1
            elif op == "usage":
                if log_id:
                    usage_logs.add(str(log_id))
            if op in ("acquire", "attach_log", "log_pending", "usage") and log_id:
                log_ids.add(str(log_id))

        capacity = state["capacity"]
        in_flight = len(outstanding)
        waiting_token = sum(1 for r in outstanding.values() if not (r or {}).get("log_id"))
        pending_usage = self._pending_usage_logs(entry_id, day)
        state.update({
            "tokens": max(0, capacity - in_flight),
            "issued_count": issued,
            "released_count": released,
            "in_flight": in_flight,
            "usage_count": issued + len(usage_logs),
            "daily_used": issued + len(usage_logs),
            "request_log_count": len(log_ids),
            "pending_log_count": waiting_token + len(pending_usage),
            "outstanding": outstanding,
        })
        return state

    def view(self, state: dict[str, Any]) -> dict[str, Any]:
        """把配额状态对象转成界面可直接展示的配额状态数据。"""
        return {
            "配额状态": f"{state['tokens']}/{state['capacity']}",
            "令牌数": state["tokens"],
            "并发上限": state["capacity"],
            "在途令牌": state["in_flight"],
            "当日已用配额": state["daily_used"],
            "日请求总量": state["daily_capacity"],
            "请求日志数": state["request_log_count"],
            "待补记日志": state["pending_log_count"],
            "数据版本": state["version"],
            "配额日期": state["day"],
        }

    def refresh_view(self, entry_id: int) -> dict[str, Any]:
        return self.view(self.ensure(entry_id))

    def policy(self) -> dict[str, str]:
        """统一口径说明，供接口与界面展示。"""
        return {
            "协议": "QST-1",
            "策略": "令牌数=并发上限-在途令牌；当日已用配额=已扣减+已补记用量，释放不回收",
            "扣减": "令牌数与当日用量在同一事务内更新，流水与状态缓存同写同回滚",
            "冲突": "按优先级仲裁，扣减(3) > 释放/挂日志(2) > 刷新(1)；低优先级让位并返回冲突",
            "刷新": "仅依据落库流水重建，缓存不一致时记录回滚并覆盖，刷新后只保留一份状态",
            "释放": "按 token_id 幂等，重复释放不会让令牌数超过并发上限",
            "仲裁": "版本冲突时以扣减为准，避免迟到的释放放宽下一次限流判断",
        }

    # ------------------------------------------------------------------
    # 提交：先落流水，再由流水生成缓存，保证两项数据同生共死
    # ------------------------------------------------------------------
    def _append_ledger(self, event: dict[str, Any]) -> None:
        ledger = self._ledger()
        event = dict(event)
        event["seq"] = (int(ledger[-1]["seq"]) + 1) if ledger else 1
        event.setdefault("day", today())
        event.setdefault("at", now_iso())
        ledger.append(event)
        store.set_pipeline_state(LEDGER_KEY, ledger)

    def _commit(
        self,
        entry_id: int,
        *,
        op: str,
        version: int,
        **extra: Any,
    ) -> dict[str, Any]:
        """原子提交：写流水 -> 按流水重建缓存 -> 整体替换状态对象。失败时全部回滚。"""
        states = self._states()
        previous_states = copy.deepcopy(states)
        ledger = self._ledger()
        ledger_len = len(ledger)
        try:
            self._append_ledger({"entry_id": entry_id, "op": op, **extra})
            rebuilt = self._rebuild(entry_id)
            rebuilt["version"] = version
            rebuilt["last_op"] = op
            rebuilt["last_op_priority"] = OP_PRIORITY.get(op, 0)
            rebuilt["updated_at"] = now_iso()
            states[str(entry_id)] = rebuilt
            store.set_pipeline_state(STATE_KEY, states)
            return copy.deepcopy(rebuilt)
        except Exception:
            store.set_pipeline_state(STATE_KEY, previous_states)
            ledger = self._ledger()
            del ledger[ledger_len:]
            store.set_pipeline_state(LEDGER_KEY, ledger)
            raise

    def _conflict(
        self,
        op: str,
        state: dict[str, Any],
        expected_version: int | None,
    ) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        """版本冲突仲裁。返回 (仲裁说明, 可继续使用的状态或 None)。"""
        if expected_version is None or int(expected_version) == int(state.get("version", 0)):
            return None, state
        incoming = OP_PRIORITY.get(op, 0)
        last_op = str(state.get("last_op") or "init")
        last_priority = OP_PRIORITY.get(last_op, int(state.get("last_op_priority", 0)))
        note = {
            "conflict": True,
            "incoming_op": op,
            "incoming_priority": incoming,
            "expected_version": int(expected_version),
            "current_version": int(state.get("version", 0)),
            "last_op": last_op,
            "last_op_priority": last_priority,
        }
        if incoming >= last_priority:
            note["decision"] = "按优先级仲裁：扣减优先，基于最新版本重试"
            note["retried"] = True
            return note, state
        note["decision"] = "按优先级仲裁：让位于更高优先级操作，本次不写入"
        note["retried"] = False
        return note, None

    # ------------------------------------------------------------------
    # 读取 / 生成 / 刷新
    # ------------------------------------------------------------------
    def ensure(self, entry_id: int) -> dict[str, Any]:
        """读取配额状态；没有或跨日时按流水重建一份，已有则原样返回。"""
        with _LOCK:
            states = self._states()
            cached = states.get(str(entry_id))
            if isinstance(cached, dict) and cached.get("day") == today():
                return copy.deepcopy(cached)
            rebuilt = self._rebuild(entry_id)
            if isinstance(cached, dict):
                rebuilt["version"] = int(cached.get("version", 0))
            states[str(entry_id)] = rebuilt
            store.set_pipeline_state(STATE_KEY, states)
            return copy.deepcopy(rebuilt)

    def snapshot(self, entry_id: int) -> dict[str, Any]:
        state = self.ensure(entry_id)
        outstanding = state.get("outstanding") or {}
        pending_token = next(
            (token_id for token_id, reservation in outstanding.items()
             if not (reservation or {}).get("log_id")),
            None,
        )
        return {
            "entry_id": entry_id,
            "version": state["version"],
            "day": state["day"],
            "tokens": state["tokens"],
            "capacity": state["capacity"],
            "daily_used": state["daily_used"],
            "daily_capacity": state["daily_capacity"],
            "in_flight": state["in_flight"],
            "request_log_count": state["request_log_count"],
            "pending_log_count": state["pending_log_count"],
            "pending_token": pending_token,
        }

    def _inconsistencies(self, cached: dict[str, Any], rebuilt: dict[str, Any]) -> list[str]:
        issues: list[str] = []
        capacity = rebuilt["capacity"]
        try:
            tokens = int(cached.get("tokens", rebuilt["tokens"]))
            daily_used = int(cached.get("daily_used", rebuilt["daily_used"]))
            in_flight = int(cached.get("in_flight", rebuilt["in_flight"]))
        except (TypeError, ValueError):
            return [ROLLBACK_DAILY_LAG]
        if tokens > capacity or tokens < 0:
            issues.append(ROLLBACK_TOKEN_OVERFLOW)
        if daily_used < rebuilt["daily_used"]:
            # 少写当日用量会让刷新后的配额状态比实际请求更宽松。
            issues.append(ROLLBACK_DAILY_LAG)
        elif daily_used > rebuilt["daily_used"]:
            issues.append(ROLLBACK_DAILY_AHEAD)
        if tokens + in_flight != capacity or in_flight != rebuilt["in_flight"]:
            issues.append(ROLLBACK_TOKENS_MISMATCH)
        if int(cached.get("request_log_count", rebuilt["request_log_count"])) != rebuilt["request_log_count"]:
            issues.append(ROLLBACK_LOG_MISMATCH)
        return issues

    def _duplicate_keys(self, states: dict[str, Any], entry_id: int) -> list[str]:
        return [
            key for key, value in states.items()
            if key != str(entry_id) and isinstance(value, dict) and value.get("entry_id") == entry_id
        ]

    def refresh(self, entry_id: int, *, expected_version: int | None = None) -> dict[str, Any]:
        """读取采集请求配额并刷新令牌状态；缓存与流水不一致时回滚缓存。"""
        with _LOCK:
            if store.find("rate_limit", entry_id) is None:
                return {"ok": False, "error": f"限流策略 {entry_id} 不存在或已归档"}
            states = self._states()
            cached = states.get(str(entry_id))
            duplicates = self._duplicate_keys(states, entry_id)
            rebuilt = self._rebuild(entry_id)
            issues: list[str] = []
            if duplicates:
                issues.append(ROLLBACK_DUPLICATE_STATE)
            if isinstance(cached, dict):
                if cached.get("day") not in (None, rebuilt["day"]):
                    issues.append(ROLLBACK_DAY_ROLLOVER)
                else:
                    issues.extend(self._inconsistencies(cached, rebuilt))
            elif rebuilt["daily_used"]:
                issues.append(ROLLBACK_DAILY_LAG)

            arbitration = None
            if expected_version is not None and isinstance(cached, dict):
                arbitration, allowed = self._conflict("refresh", cached, expected_version)
                if allowed is None:
                    return {
                        "ok": False, "conflict": True, "arbitration": arbitration,
                        "state": copy.deepcopy(cached),
                    }

            # 刷新后只保留一份配额状态：清理重复键，再整体重写。
            previous_states = copy.deepcopy(states)
            for key in duplicates:
                states.pop(key, None)

            version = int(cached.get("version", 0)) if isinstance(cached, dict) else 0
            if not issues:
                rebuilt["version"] = version
                rebuilt["last_op"] = str((cached or {}).get("last_op") or "refresh")
                rebuilt["last_op_priority"] = int((cached or {}).get("last_op_priority") or 0)
                rebuilt["updated_at"] = now_iso()
                states[str(entry_id)] = rebuilt
                store.set_pipeline_state(STATE_KEY, states)
                return {
                    "ok": True, "rolled_back": False, "issues": [],
                    "arbitration": arbitration, "state": copy.deepcopy(rebuilt),
                }

            snapshot_before = {
                "tokens": (cached or {}).get("tokens"),
                "daily_used": (cached or {}).get("daily_used"),
                "in_flight": (cached or {}).get("in_flight"),
                "request_log_count": (cached or {}).get("request_log_count"),
                "version": (cached or {}).get("version"),
            }
            try:
                committed = self._commit(
                    entry_id, op="rollback", version=version + 1, 
                    issues=issues, before=snapshot_before,
                )
                removed = self._duplicate_keys(self._states(), entry_id)
                latest = self._states()
                for key in removed:
                    latest.pop(key, None)
                store.set_pipeline_state(STATE_KEY, latest)
            except Exception:
                store.set_pipeline_state(STATE_KEY, previous_states)
                raise
            return {
                "ok": True,
                "rolled_back": True,
                "issues": issues,
                "arbitration": arbitration,
                "state": committed,
            }

    def check(self, entry_id: int) -> dict[str, Any]:
        """只读核对：把配额状态与落库流水逐项比对，不改写任何数据。"""
        with _LOCK:
            state = self.ensure(entry_id)
            rebuilt = self._rebuild(entry_id)
            issues = self._inconsistencies(state, rebuilt) if state.get("version") else []
            return {
                "ok": not issues,
                "issues": issues,
                "state": self.snapshot(entry_id),
                "ledger_events": len(self._events_for_day(entry_id, state["day"])),
            }

    # ------------------------------------------------------------------
    # 扣减 / 释放 / 挂日志 / 补记用量
    # ------------------------------------------------------------------
    def _reject(self, state: dict[str, Any], reason: str, **extra: Any) -> dict[str, Any]:
        result = {
            "allowed": False,
            "reason": reason,
            "tokens": state["tokens"],
            "daily_used": state["daily_used"],
            "capacity": state["capacity"],
            "daily_capacity": state["daily_capacity"],
            "version": state["version"],
            "request_log_count": state["request_log_count"],
        }
        result.update(extra)
        return result

    def acquire(
        self,
        entry_id: int,
        *,
        request_log_id: str | None = None,
        session_id: int | None = None,
        order: str = ORDER_QUOTA_FIRST,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """原子扣减：``tokens`` 与 ``daily_used`` 必须在同一次提交里一起变化。"""
        order = order if order in WRITE_ORDERS else ORDER_QUOTA_FIRST
        with _LOCK:
            entry = store.find("rate_limit", entry_id)
            if entry is None or entry.get("status") != "已生效":
                return {"allowed": False, "reason": "策略不存在或未生效"}
            state = self.ensure(entry_id)
            arbitration, usable = self._conflict("acquire", state, expected_version)
            if usable is None:
                return self._reject(state, "配额状态版本冲突，请重试", conflict=True, arbitration=arbitration)
            state = usable
            if state["daily_used"] >= state["daily_capacity"]:
                return self._reject(state, "日配额已耗尽", arbitration=arbitration)
            if state["tokens"] <= 0:
                return self._reject(state, "并发令牌耗尽", arbitration=arbitration)

            token_id = f"TK-{entry_id}-{int(state['issued_count']) + 1}"
            outstanding = copy.deepcopy(state.get("outstanding") or {})
            outstanding[token_id] = {
                "token_id": token_id,
                "log_id": request_log_id,
                "session_id": session_id,
                "order": order,
                "at": now_iso(),
            }
            committed = self._commit(
                entry_id, op="acquire", version=int(state["version"]) + 1,
                token_id=token_id, log_id=request_log_id, session_id=session_id, order=order,
            )
            if request_log_id:
                links = self._links()
                links[str(request_log_id)] = {
                    "entry_id": entry_id, "token_id": token_id,
                    "session_id": session_id, "status": "linked",
                }
                store.set_pipeline_state(LINK_KEY, links)
            return {
                "allowed": True,
                "tokens": committed["tokens"],
                "daily_used": committed["daily_used"],
                "capacity": committed["capacity"],
                "daily_capacity": committed["daily_capacity"],
                "token_id": token_id,
                "version": committed["version"],
                "request_log_count": committed["request_log_count"],
                "request_log_id": request_log_id,
                "order": order,
                "state": committed,
                "arbitration": arbitration,
            }

    def attach_request_log(
        self,
        entry_id: int,
        token_id: str,
        request_log_id: str,
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """先扣配额时请求日志晚到：把日志编号补挂到对应令牌上。"""
        with _LOCK:
            state = self.ensure(entry_id)
            arbitration, usable = self._conflict("attach_log", state, expected_version)
            if usable is None:
                return {"ok": False, "conflict": True, "arbitration": arbitration, "state": state}
            state = usable
            reservation = (state.get("outstanding") or {}).get(str(token_id))
            if reservation is None:
                return {"ok": False, "error": "令牌不存在或已释放", "state": state}
            if reservation.get("log_id") == request_log_id:
                return {"ok": True, "already_linked": True, "state": state,
                        "arbitration": arbitration}
            committed = self._commit(
                entry_id, op="attach_log", version=int(state["version"]) + 1,
                token_id=token_id, log_id=request_log_id,
            )
            links = self._links()
            links[str(request_log_id)] = {
                "entry_id": entry_id, "token_id": token_id, "status": "linked",
            }
            store.set_pipeline_state(LINK_KEY, links)
            return {"ok": True, "state": committed, "arbitration": arbitration}

    def mark_log_pending(self, entry_id: int, request_log_id: str, *, session_id: int | None = None) -> dict[str, Any]:
        """先记录请求但扣减未成功：日志保留，标记待补记用量。"""
        with _LOCK:
            state = self.ensure(entry_id)
            committed = self._commit(
                entry_id, op="log_pending", version=int(state["version"]) + 1,
                log_id=request_log_id, session_id=session_id,
            )
            links = self._links()
            links[str(request_log_id)] = {
                "entry_id": entry_id, "token_id": None, "session_id": session_id,
                "status": "pending_usage",
            }
            store.set_pipeline_state(LINK_KEY, links)
            return {"ok": True, "state": committed}

    def reconcile_pending_usage(self, entry_id: int) -> dict[str, Any]:
        """释放前后补齐用量写入：把已保留但未计量的日志补进当日用量。"""
        with _LOCK:
            state = self.ensure(entry_id)
            pending_logs = self._pending_usage_logs(entry_id, state["day"])
            completed = 0
            for log_id in pending_logs:
                current = self.ensure(entry_id)
                links = self._links()
                self._commit(
                    entry_id, op="usage", version=int(current["version"]) + 1,
                    log_id=log_id,
                )
                link = links.get(str(log_id)) or {}
                link.update({"entry_id": entry_id, "status": "usage_posted"})
                links[str(log_id)] = link
                store.set_pipeline_state(LINK_KEY, links)
                completed += 1
            return {
                "ok": True, "completed": completed,
                "state": self.snapshot(entry_id),
            }

    def release(
        self,
        entry_id: int,
        *,
        token_id: str | None = None,
        request_log_id: str | None = None,
        expected_version: int | None = None,
        reconcile_usage: bool = True,
    ) -> dict[str, Any]:
        """释放令牌；同一 token_id 重复释放幂等，令牌永远不会超过并发上限。"""
        with _LOCK:
            entry = store.find("rate_limit", entry_id)
            if entry is None:
                return {"ok": False, "error": "策略不存在或已归档"}
            state = self.ensure(entry_id)
            arbitration, usable = self._conflict("release", state, expected_version)
            if usable is None:
                return {
                    "ok": False, "conflict": True, "arbitration": arbitration,
                    "tokens": state["tokens"], "version": state["version"],
                }
            state = usable
            outstanding = state.get("outstanding") or {}
            if token_id is None and outstanding:
                token_id = next(iter(outstanding))
            reservation = outstanding.get(str(token_id)) if token_id else None
            links = self._links()

            if token_id and reservation is None:
                # 同一 token 的重复释放：幂等返回，令牌数不会因重复释放超过并发上限。
                return {
                    "ok": True, "already_released": True, "token_id": token_id,
                    "tokens": state["tokens"], "daily_used": state["daily_used"],
                    "capacity": state["capacity"], "version": state["version"],
                    "request_log_count": state["request_log_count"],
                }
            if token_id is None:
                return {
                    "ok": True, "already_released": True, "reason": "无待释放令牌",
                    "tokens": state["tokens"], "version": state["version"],
                    "request_log_count": state["request_log_count"],
                }

            remaining = copy.deepcopy(outstanding)
            remaining.pop(str(token_id), None)
            committed = self._commit(
                entry_id, op="release", version=int(state["version"]) + 1,
                token_id=token_id, log_id=request_log_id or (reservation or {}).get("log_id"),
            )
            released_log = request_log_id or (reservation or {}).get("log_id")
            if released_log:
                link = links.get(str(released_log)) or {}
                link.update({"entry_id": entry_id, "token_id": token_id, "status": "released"})
                links[str(released_log)] = link
            store.set_pipeline_state(LINK_KEY, links)

            usage_completed = 0
            if reconcile_usage:
                usage_completed = self.reconcile_pending_usage(entry_id).get("completed", 0)
            latest = self.ensure(entry_id)
            return {
                "ok": True,
                "tokens": latest["tokens"],
                "daily_used": latest["daily_used"],
                "capacity": latest["capacity"],
                "version": latest["version"],
                "token_id": token_id,
                "request_log_count": latest["request_log_count"],
                "usage_completed": usage_completed,
                "state": latest,
                "arbitration": arbitration,
            }

    # ------------------------------------------------------------------
    # 版本冲突演示入口：同一版本上释放与下一次扣减并发
    # ------------------------------------------------------------------
    def arbitrate_release_then_acquire(
        self,
        entry_id: int,
        token_id: str | None,
        *,
        expected_version: int | None = None,
    ) -> dict[str, Any]:
        """演示释放与下一次扣减同时命中同一版本时的仲裁结果。

        两者都以当前版本作为 expected_version。扣减优先级更高：若释放先落库，扣减基于
        释放后的新版本继续；若扣减先落库，释放按同一版本判断时会看到更高优先级操作
        已经占位，于是让位并返回冲突，不会放宽下一次限流判断。
        """
        base = self.ensure(entry_id)
        expected = int(expected_version) if expected_version is not None else int(base["version"])
        release_result = self.release(
            entry_id, token_id=token_id, expected_version=expected, reconcile_usage=False)
        if release_result.get("ok"):
            # 释放成功会推进版本，扣减基于释放后的新版本继续；释放让位（冲突）时，
            # 扣减仍用调用方看到的版本发起并按优先级仲裁。
            next_expected = int(release_result.get("version", expected))
            acquire_result = self.acquire(
                entry_id, order=ORDER_QUOTA_FIRST, expected_version=next_expected)
            acquire_result["expected_version_used"] = next_expected
        else:
            acquire_result = self.acquire(
                entry_id, order=ORDER_QUOTA_FIRST, expected_version=expected)
            acquire_result["expected_version_used"] = expected
        return {
            "expected_version": expected,
            "release": release_result,
            "acquire": acquire_result,
            "state": self.snapshot(entry_id),
        }


quota_service = QuotaStateService()
