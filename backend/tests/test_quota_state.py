"""配额状态数据测试：原子扣减、版本仲裁、刷新回滚与跨模块核对。"""
from __future__ import annotations

import pytest

from app.seed import SEED_ROWS
from app.services.quota import (
    ORDER_LOG_FIRST,
    ORDER_QUOTA_FIRST,
    ROLLBACK_DAILY_LAG,
    ROLLBACK_DUPLICATE_STATE,
    ROLLBACK_TOKEN_OVERFLOW,
    quota_service,
)
from app.services.rate_limit import RateLimitService
from app.services.session_mgr import SessionMgrService
from app.store import Store, store


@pytest.fixture(autouse=True)
def _reset_store():
    """每个用例前重置示例数据与跨模块状态。"""
    store._tables.clear()
    store._tables.update({name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()})
    store._pipeline_states.clear()
    store._pipeline_states.update(Store()._pipeline_states)
    store._relations.clear()
    store._build_relations()
    yield


def _active_policy(capacity: int = 2, daily: int = 10) -> tuple[RateLimitService, int]:
    svc = RateLimitService()
    entry, missing = svc.create_entry({
        "策略编号": "RATE-QUOTA-01",
        "目标源": "SRC-ELEC-001",
        "并发上限": str(capacity),
        "日请求总量": str(daily),
    })
    assert not missing and entry is not None
    assert svc.run_action(entry["id"], "标记生效")[0] is not None
    return svc, entry["id"]


def test_quota_generated_on_create_matches_policy() -> None:
    svc, entry_id = _active_policy(capacity=3, daily=8)
    quota = svc.get_entry(entry_id)["_quota_state"]
    assert quota["令牌数"] == 3
    assert quota["并发上限"] == 3
    assert quota["当日已用配额"] == 0
    assert quota["日请求总量"] == 8
    assert quota["请求日志数"] == 0
    assert quota_service.check(entry_id)["ok"] is True


def test_acquire_updates_tokens_and_daily_usage_atomically() -> None:
    svc, entry_id = _active_policy(capacity=2, daily=10)

    result = svc.acquire_token(entry_id)
    assert result["allowed"] is True
    assert result["tokens"] == 1
    assert result["daily_used"] == 1

    # 落库流水里只有一条扣减事件；令牌数和当日用量来自同一次提交。
    events = [e for e in store.pipeline_state("rate_limit_ledger") if e["entry_id"] == entry_id]
    assert [e["op"] for e in events] == ["acquire"]
    state = svc.get_entry(entry_id)["_quota_state"]
    assert (state["令牌数"], state["当日已用配额"]) == (1, 1)
    assert state["令牌数"] + state["在途令牌"] == state["并发上限"]


def test_refresh_keeps_token_count_and_daily_usage_in_sync() -> None:
    svc, entry_id = _active_policy(capacity=4, daily=10)
    svc.acquire_token(entry_id)
    svc.acquire_token(entry_id)

    refreshed = svc.refresh_quota(entry_id)
    assert refreshed["ok"] is True
    assert refreshed["rolled_back"] is False
    quota = refreshed["quota_state"]
    assert quota["令牌数"] == 2
    assert quota["当日已用配额"] == 2
    assert quota["在途令牌"] == 2
    assert quota["请求日志数"] == 0

    # 重新查询仍然得到同一份结果。
    assert svc.get_entry(entry_id)["_quota_state"] == quota


def test_log_first_preserves_log_when_quota_exhausted_then_posts_usage() -> None:
    svc, entry_id = _active_policy(capacity=1, daily=5)
    session = SessionMgrService()
    assert svc.acquire_token(entry_id, order=ORDER_QUOTA_FIRST)["allowed"] is True

    recorded = session.record_request(
        1, "https://example.com/a", 120.0, rate_limit_id=entry_id, order=ORDER_LOG_FIRST)
    quota = recorded["quota"]
    assert quota["allowed"] is False
    assert quota["reason"] == "并发令牌耗尽"
    assert quota["pending_usage"] is True
    assert recorded["request_log_id"] in {
        log.get("log_id") for log in session.get_entry(1)["_request_log"]
    }

    # 释放前后单独补齐用量：请求日志、令牌数、当日用量可以彼此核对。
    released = session.release_request(1, request_log_id=recorded["request_log_id"])
    assert released["ok"] is True
    assert released["usage_completed"] >= 1
    assert released["quota"]["tokens"] == 1
    assert released["quota"]["daily_used"] == 2
    assert released["quota"]["request_log_count"] == 1
    assert quota_service.check(entry_id)["ok"] is True


def test_quota_first_shows_new_tokens_then_attaches_log_id() -> None:
    svc, entry_id = _active_policy(capacity=2, daily=5)
    session = SessionMgrService()
    recorded = session.record_request(
        1, "https://example.com/b", 80.0, rate_limit_id=entry_id, order=ORDER_QUOTA_FIRST)
    quota = recorded["quota"]
    assert quota["allowed"] is True
    assert quota["consistent"] is True
    assert quota["before"]["tokens"] == 2
    assert quota["after"]["tokens"] == 1
    assert quota["after"]["request_log_count"] == 1
    reservation = quota["state"]["outstanding"][quota["token_id"]]
    assert reservation["log_id"] == "REQ-1-843"

    # 刷新后配额状态仍然指向同一个请求日志编号。
    refreshed = svc.refresh_quota(entry_id)["quota_state"]
    assert refreshed["令牌数"] == 1
    assert refreshed["请求日志数"] == 1


def test_duplicate_release_is_idempotent_and_never_exceeds_capacity() -> None:
    svc, entry_id = _active_policy(capacity=2, daily=5)
    session = SessionMgrService()
    recorded = session.record_request(
        1, "https://example.com/c", 50.0, rate_limit_id=entry_id, order=ORDER_QUOTA_FIRST)
    token_id = recorded["quota"]["token_id"]

    first = session.release_request(1, request_log_id=recorded["request_log_id"], token_id=token_id)
    second = session.release_request(1, request_log_id=recorded["request_log_id"], token_id=token_id)
    third = svc.release_token(entry_id, token_id=token_id)
    assert first["ok"] is True
    assert second.get("already_released") is True
    assert third.get("already_released") is True

    quota = svc.refresh_quota(entry_id)["quota_state"]
    assert quota["令牌数"] == 2
    assert quota["在途令牌"] == 0
    assert quota["令牌数"] <= quota["并发上限"]
    assert quota["当日已用配额"] == 1  # 释放不回收当日用量


def test_refresh_rolls_back_cache_that_only_updated_tokens() -> None:
    svc, entry_id = _active_policy(capacity=3, daily=5)
    svc.acquire_token(entry_id)

    # 模拟只落令牌、不落当日用量的缓存：比实际请求更宽松。
    states = store.pipeline_state("rate_limit_states")
    stale = dict(states[str(entry_id)])
    stale["tokens"] = 3
    stale["daily_used"] = 0
    stale["in_flight"] = 0
    states[str(entry_id)] = stale
    store.set_pipeline_state("rate_limit_states", states)

    result = svc.refresh_quota(entry_id)
    assert result["rolled_back"] is True
    assert ROLLBACK_DAILY_LAG in result["issues"]
    assert result["quota_state"]["令牌数"] == 2
    assert result["quota_state"]["当日已用配额"] == 1
    logs = [e for e in store.pipeline_state("rate_limit_ledger") if e["op"] == "rollback"]
    assert logs and logs[-1]["issues"]


def test_refresh_rolls_back_token_overflow_and_keeps_single_state() -> None:
    svc, entry_id = _active_policy(capacity=2, daily=5)
    states = store.pipeline_state("rate_limit_states")
    states[str(entry_id)]["tokens"] = 99
    states["legacy-copy"] = dict(states[str(entry_id)])
    store.set_pipeline_state("rate_limit_states", states)

    result = svc.refresh_quota(entry_id)
    assert result["rolled_back"] is True
    assert ROLLBACK_TOKEN_OVERFLOW in result["issues"]
    assert ROLLBACK_DUPLICATE_STATE in result["issues"]
    states = store.pipeline_state("rate_limit_states")
    assert "legacy-copy" not in states
    assert states[str(entry_id)]["tokens"] == 2


def test_version_conflict_arbitration_prefers_acquire() -> None:
    svc, entry_id = _active_policy(capacity=3, daily=10)
    first = svc.acquire_token(entry_id, order=ORDER_QUOTA_FIRST)
    base_version = first["version"]

    # 扣减先落库，迟到的释放拿着旧版本进来，按优先级让位。
    later = svc.acquire_token(entry_id, order=ORDER_QUOTA_FIRST)
    stale_release = svc.release_token(entry_id, token_id=first["token_id"], expected_version=base_version)
    assert stale_release["ok"] is False
    assert stale_release["conflict"] is True
    assert stale_release["arbitration"]["incoming_op"] == "release"
    assert stale_release["arbitration"]["retried"] is False

    quota = svc.get_entry(entry_id)["_quota_state"]
    assert quota["令牌数"] == 1  # 两次扣减都生效，没有被迟到的释放放宽
    assert quota["在途令牌"] == 2
    assert quota["数据版本"] > later["version"] or quota["数据版本"] == later["version"]


def test_release_then_acquire_on_same_object_advances_version() -> None:
    svc, entry_id = _active_policy(capacity=2, daily=10)
    held = svc.acquire_token(entry_id, order=ORDER_QUOTA_FIRST)

    arbitration = quota_service.arbitrate_release_then_acquire(entry_id, held["token_id"])
    assert arbitration["release"]["ok"] is True
    assert arbitration["acquire"]["allowed"] is True
    assert arbitration["release"]["version"] < arbitration["acquire"]["version"]
    quota = arbitration["state"]
    assert quota["tokens"] == 1
    assert quota["daily_used"] == 2
    assert quota["in_flight"] == 1


def test_consecutive_acquire_release_refresh_stays_consistent() -> None:
    svc, entry_id = _active_policy(capacity=3, daily=10)
    session = SessionMgrService()
    log_ids = []

    for index in range(3):
        recorded = session.record_request(
            1, f"https://example.com/{index}", 30.0 + index,
            rate_limit_id=entry_id, order=ORDER_QUOTA_FIRST)
        assert recorded["quota"]["allowed"] is True
        log_ids.append(recorded["request_log_id"])
        released = session.release_request(1, request_log_id=recorded["request_log_id"])
        assert released["ok"] is True
        refreshed = svc.refresh_quota(entry_id)
        assert refreshed["rolled_back"] is False
        quota = refreshed["quota_state"]
        assert quota["令牌数"] == 3
        assert quota["在途令牌"] == 0
        assert quota["请求日志数"] == index + 1

    final = svc.refresh_quota(entry_id)["quota_state"]
    assert final["当日已用配额"] == 3
    assert final["请求日志数"] == 3
    assert final["令牌数"] == final["并发上限"]
    reports = session.get_entry(1)["_quota_checks"]
    assert len(reports) == 3
    assert all(report["quota"]["consistent"] for report in reports)


def test_session_resolves_rate_limit_through_source_relation() -> None:
    session = SessionMgrService()
    entry = session.get_entry(1)
    assert session.resolve_rate_limit(entry) == 1
    recorded = session.record_request(1, "https://example.com/d", 10.0)
    assert recorded["rate_limit_id"] == 1
    assert recorded["quota"]["allowed"] is True
    assert recorded["quota"]["consistent"] is True
    saved = session.get_entry(1)["_quota_state"]
    assert saved["请求日志数"] >= 1
