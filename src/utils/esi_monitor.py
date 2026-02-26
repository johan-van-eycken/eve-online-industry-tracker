from __future__ import annotations

import threading
import time
from collections import Counter, defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Deque, Dict, List, Optional


@dataclass(frozen=True)
class EsiIssue:
    ts: float
    kind: str  # "warning" | "error" | "exception"
    method: str
    endpoint: str
    url: str
    status_code: Optional[int]
    message: str
    retry_after_seconds: Optional[float]
    error_limit_remain: Optional[int]
    error_limit_reset_seconds: Optional[int]


class ESIMonitor:
    """In-process ESI call monitor (per Python process).

    Notes:
    - Counts *HTTP attempts* (including retries and pagination pages).
    - Intended to be extremely lightweight and safe to call everywhere.
    - Data is in-memory only (resets on process restart).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = time.time()

        self._total_calls: int = 0
        self._calls_by_route: Counter[str] = Counter()
        self._calls_by_status: Counter[str] = Counter()

        # Route-level reliability.
        self._success_by_route: Counter[str] = Counter()
        self._error_by_route: Counter[str] = Counter()

        # Latency samples (bounded).
        self._latencies_ms: Deque[float] = deque(maxlen=5000)
        self._latencies_ms_by_route: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=500))

        # Rolling time series: bucket_epoch_seconds -> count and latency aggregates.
        self._bucket_counts: Dict[int, int] = defaultdict(int)
        self._bucket_latency_sum_ms: Dict[int, float] = defaultdict(float)
        self._bucket_latency_count: Dict[int, int] = defaultdict(int)

        # Retry / pagination / cache counters.
        self._retry_events: int = 0
        self._retry_by_reason: Counter[str] = Counter()
        self._retry_sleep_seconds_total: float = 0.0

        self._sleep_seconds_total: float = 0.0
        self._sleep_seconds_by_kind: Dict[str, float] = defaultdict(float)

        self._pagination_pages_total: int = 0
        self._pagination_pages_by_route: Counter[str] = Counter()

        self._cache_counts: Counter[str] = Counter()

        self._exceptions_by_type: Counter[str] = Counter()

        # Recent issues (errors/warnings/exceptions)
        self._issues: Deque[EsiIssue] = deque(maxlen=500)

    def record_sleep(
        self,
        *,
        kind: str,
        seconds: float,
        context: str = "",
        method: Optional[str] = None,
        endpoint: Optional[str] = None,
        url: str = "",
        reason: str = "",
    ) -> None:
        try:
            s = float(seconds)
        except Exception:
            return
        if s <= 0:
            return

        kind_s = str(kind or "").strip().lower() or "sleep"
        method_s = str(method or "").upper() if method is not None else ""
        endpoint_s = str(endpoint or "") if endpoint is not None else ""
        route_key = f"{method_s} {endpoint_s}".strip() if (method_s or endpoint_s) else ""

        with self._lock:
            self._sleep_seconds_total += s
            self._sleep_seconds_by_kind[kind_s] += s
            if kind_s == "retry":
                self._retry_sleep_seconds_total += s
                self._retry_by_reason[str(reason or "") or "?"] += 1
            if kind_s == "gate":
                # Nothing extra; aggregated above.
                pass
            if kind_s == "pagination" and route_key:
                # This is pacing sleep, not page count.
                pass

    def record_retry_event(
        self,
        *,
        reason: str,
        sleep_seconds: float,
        method: Optional[str] = None,
        endpoint: Optional[str] = None,
        url: str = "",
    ) -> None:
        with self._lock:
            self._retry_events += 1
        self.record_sleep(
            kind="retry",
            seconds=sleep_seconds,
            method=method,
            endpoint=endpoint,
            url=url,
            reason=str(reason or "") or "?",
        )

    @property
    def started_at(self) -> float:
        return self._started_at

    def record_http_attempt(
        self,
        *,
        method: str,
        endpoint: str,
        url: str,
        status_code: Optional[int],
        elapsed_ms: Optional[float],
        headers: Any = None,
        exception: Optional[BaseException] = None,
        cache_mode: Optional[str] = None,
        page: Optional[int] = None,
        bucket_seconds: int = 5,
    ) -> None:
        now = time.time()
        method_s = str(method or "").upper() or "?"
        endpoint_s = str(endpoint or "")
        url_s = str(url or "")
        route_key = f"{method_s} {endpoint_s}".strip()

        # Extract some headers we care about.
        retry_after: Optional[float] = None
        remain: Optional[int] = None
        reset: Optional[int] = None
        warning_header: Optional[str] = None
        try:
            if headers is not None:
                ra = headers.get("Retry-After")
                if ra is not None:
                    try:
                        retry_after = float(ra)
                    except Exception:
                        retry_after = None

                er = headers.get("X-Esi-Error-Limit-Remain")
                if er is not None:
                    try:
                        remain = int(er)
                    except Exception:
                        remain = None

                es = headers.get("X-Esi-Error-Limit-Reset")
                if es is not None:
                    try:
                        reset = int(es)
                    except Exception:
                        reset = None

                wh = headers.get("Warning")
                if wh is not None:
                    warning_header = str(wh)
        except Exception:
            # Never let monitoring break requests.
            pass

        # Classification.
        kind: Optional[str] = None
        message: Optional[str] = None
        if exception is not None:
            kind = "exception"
            message = str(exception)
        elif status_code is not None:
            try:
                sc = int(status_code)
            except Exception:
                sc = None
            if sc is not None:
                if 500 <= sc:
                    kind = "error"
                    message = f"HTTP {sc}"
                elif sc in (420, 429):
                    kind = "warning"
                    message = f"HTTP {sc} (throttle)"
                elif 400 <= sc:
                    kind = "error"
                    message = f"HTTP {sc}"
                elif warning_header:
                    kind = "warning"
                    message = warning_header

        # Bucket for the time series.
        b = int(max(1, int(bucket_seconds)))
        bucket = int(now // b) * b

        # Normalize cache mode.
        cache_mode_s = str(cache_mode or "").strip().lower()
        if cache_mode_s not in ("", "off", "enabled_no_entry", "enabled_with_entry"):
            cache_mode_s = ""

        # Normalize elapsed.
        latency_ms: Optional[float]
        try:
            latency_ms = float(elapsed_ms) if elapsed_ms is not None else None
        except Exception:
            latency_ms = None
        if latency_ms is not None and latency_ms < 0:
            latency_ms = None

        with self._lock:
            self._total_calls += 1
            if route_key:
                self._calls_by_route[route_key] += 1

            if status_code is None:
                self._calls_by_status["exception"] += 1
            else:
                self._calls_by_status[str(status_code)] += 1

            # Success/error split (attempt-based).
            if status_code is None:
                if route_key:
                    self._error_by_route[route_key] += 1
            else:
                try:
                    sc_i = int(status_code)
                except Exception:
                    sc_i = -1
                if 200 <= sc_i < 400 or sc_i == 304:
                    if route_key:
                        self._success_by_route[route_key] += 1
                else:
                    if route_key:
                        self._error_by_route[route_key] += 1

            # Latency sampling + bucket aggregates.
            if latency_ms is not None:
                self._latencies_ms.append(latency_ms)
                if route_key:
                    self._latencies_ms_by_route[route_key].append(latency_ms)
                self._bucket_latency_sum_ms[bucket] += float(latency_ms)
                self._bucket_latency_count[bucket] += 1

            # Cache counters (attempt-based).
            if cache_mode_s:
                self._cache_counts[f"cache_mode:{cache_mode_s}"] += 1
                if cache_mode_s != "off":
                    self._cache_counts["cache_enabled_attempts"] += 1
                    if cache_mode_s == "enabled_with_entry":
                        self._cache_counts["cache_has_entry_attempts"] += 1
                    if status_code == 304:
                        self._cache_counts["cache_304"] += 1
                    if status_code == 200:
                        self._cache_counts["cache_200"] += 1

            # Pagination counters.
            if page is not None:
                self._pagination_pages_total += 1
                if route_key:
                    self._pagination_pages_by_route[route_key] += 1

            # Exception breakdown.
            if exception is not None:
                self._exceptions_by_type[type(exception).__name__] += 1

            self._bucket_counts[bucket] += 1

            if kind is not None and message is not None:
                self._issues.append(
                    EsiIssue(
                        ts=now,
                        kind=kind,
                        method=method_s,
                        endpoint=endpoint_s,
                        url=url_s,
                        status_code=status_code,
                        message=message,
                        retry_after_seconds=retry_after,
                        error_limit_remain=remain,
                        error_limit_reset_seconds=reset,
                    )
                )

            # Prune old buckets (keep ~2h by default to avoid unbounded growth).
            prune_before = int(now) - (2 * 3600)
            if len(self._bucket_counts) > 1800:
                for k in list(self._bucket_counts.keys()):
                    if k < prune_before:
                        self._bucket_counts.pop(k, None)
                        self._bucket_latency_sum_ms.pop(k, None)
                        self._bucket_latency_count.pop(k, None)

            # Prune any route latency deques that have gone cold.
            if len(self._latencies_ms_by_route) > 1000:
                for rk in list(self._latencies_ms_by_route.keys()):
                    if rk not in self._calls_by_route:
                        self._latencies_ms_by_route.pop(rk, None)

    def snapshot(
        self,
        *,
        window_seconds: int = 900,
        bucket_seconds: int = 5,
        top_n: int = 20,
    ) -> dict:
        now = time.time()
        window = int(max(10, window_seconds))
        bucket = int(max(1, bucket_seconds))
        top = int(max(1, top_n))

        with self._lock:
            started_at = float(self._started_at)
            total_calls = int(self._total_calls)
            calls_by_status = dict(self._calls_by_status)
            success_by_route = dict(self._success_by_route)
            error_by_route = dict(self._error_by_route)

            latency_samples = list(self._latencies_ms)
            latencies_by_route = {k: list(v) for k, v in self._latencies_ms_by_route.items()}

            retry_events = int(self._retry_events)
            retry_by_reason = dict(self._retry_by_reason)
            retry_sleep_seconds_total = float(self._retry_sleep_seconds_total)

            sleep_seconds_total = float(self._sleep_seconds_total)
            sleep_seconds_by_kind = dict(self._sleep_seconds_by_kind)

            pagination_pages_total = int(self._pagination_pages_total)
            pagination_pages_by_route = dict(self._pagination_pages_by_route)

            cache_counts = dict(self._cache_counts)
            exceptions_by_type = dict(self._exceptions_by_type)

            # Timeseries for the selected window.
            start_bucket = int((now - window) // bucket) * bucket
            end_bucket = int(now // bucket) * bucket
            ts: List[dict] = []
            t = start_bucket
            while t <= end_bucket:
                calls = int(self._bucket_counts.get(int(t), 0))
                l_count = int(self._bucket_latency_count.get(int(t), 0))
                l_sum = float(self._bucket_latency_sum_ms.get(int(t), 0.0))
                avg_ms = (l_sum / l_count) if l_count > 0 else None
                ts.append({"ts": int(t), "calls": calls, "avg_latency_ms": avg_ms})
                t += bucket

            top_routes = [
                {"route": k, "count": int(v)}
                for k, v in self._calls_by_route.most_common(top)
            ]

            issues = [
                {
                    "ts": float(i.ts),
                    "kind": i.kind,
                    "method": i.method,
                    "endpoint": i.endpoint,
                    "url": i.url,
                    "status_code": i.status_code,
                    "message": i.message,
                    "retry_after_seconds": i.retry_after_seconds,
                    "error_limit_remain": i.error_limit_remain,
                    "error_limit_reset_seconds": i.error_limit_reset_seconds,
                }
                for i in list(self._issues)
            ]

        # Some derived counts (outside lock).
        def _status_count(pred: Callable[[int], bool]) -> int:
            out = 0
            for k, v in calls_by_status.items():
                try:
                    sc = int(k)
                except Exception:
                    continue
                if pred(sc):
                    out += int(v)
            return out

        warnings_total = _status_count(lambda sc: sc in (420, 429))
        errors_total = _status_count(lambda sc: 400 <= sc) + int(calls_by_status.get("exception", 0))

        def _pct(values: List[float], p: float) -> Optional[float]:
            if not values:
                return None
            if p <= 0:
                return float(min(values))
            if p >= 100:
                return float(max(values))
            xs = sorted(values)
            k = (len(xs) - 1) * (p / 100.0)
            f = int(k)
            c = min(f + 1, len(xs) - 1)
            if f == c:
                return float(xs[f])
            d0 = xs[f] * (c - k)
            d1 = xs[c] * (k - f)
            return float(d0 + d1)

        latency_avg = (sum(latency_samples) / len(latency_samples)) if latency_samples else None
        latency_p50 = _pct(latency_samples, 50)
        latency_p95 = _pct(latency_samples, 95)

        # Top slowest routes by p95 latency.
        slow_rows: List[dict] = []
        for route, vals in latencies_by_route.items():
            if not vals:
                continue
            slow_rows.append(
                {
                    "route": route,
                    "count": int(len(vals)),
                    "avg_ms": float(sum(vals) / len(vals)) if vals else None,
                    "p50_ms": _pct(vals, 50),
                    "p95_ms": _pct(vals, 95),
                    "success": int(success_by_route.get(route, 0)),
                    "error": int(error_by_route.get(route, 0)),
                }
            )
        slow_rows.sort(key=lambda r: float(r.get("p95_ms") or 0.0), reverse=True)
        top_slowest_routes = slow_rows[: max(1, int(top_n))]

        # Cache ratios.
        cache_enabled_attempts = int(cache_counts.get("cache_enabled_attempts", 0))
        cache_304 = int(cache_counts.get("cache_304", 0))
        cache_200 = int(cache_counts.get("cache_200", 0))
        cache_hit_ratio = (cache_304 / cache_enabled_attempts) if cache_enabled_attempts > 0 else None

        # Overall success rate.
        success_total = 0
        error_total = 0
        for k, v in calls_by_status.items():
            if k == "exception":
                error_total += int(v)
                continue
            try:
                sc = int(k)
            except Exception:
                continue
            if 200 <= sc < 400 or sc == 304:
                success_total += int(v)
            else:
                error_total += int(v)
        success_rate = (success_total / (success_total + error_total)) if (success_total + error_total) > 0 else None

        return {
            "started_at": started_at,
            "now": float(now),
            "window_seconds": window,
            "bucket_seconds": bucket,
            "totals": {
                "calls": total_calls,
                "warnings": int(warnings_total),
                "errors": int(errors_total),
            },
            "success": {
                "success_total": int(success_total),
                "error_total": int(error_total),
                "success_rate": float(success_rate) if success_rate is not None else None,
            },
            "latency": {
                "avg_ms": float(latency_avg) if latency_avg is not None else None,
                "p50_ms": float(latency_p50) if latency_p50 is not None else None,
                "p95_ms": float(latency_p95) if latency_p95 is not None else None,
                "samples": int(len(latency_samples)),
            },
            "sleep": {
                "total_seconds": float(sleep_seconds_total),
                "by_kind_seconds": sleep_seconds_by_kind,
                "retry_sleep_seconds": float(retry_sleep_seconds_total),
            },
            "retries": {
                "events": int(retry_events),
                "by_reason": retry_by_reason,
            },
            "pagination": {
                "pages_total": int(pagination_pages_total),
                "pages_by_route": pagination_pages_by_route,
            },
            "cache": {
                "counts": cache_counts,
                "enabled_attempts": int(cache_enabled_attempts),
                "cache_304": int(cache_304),
                "cache_200": int(cache_200),
                "hit_ratio": float(cache_hit_ratio) if cache_hit_ratio is not None else None,
            },
            "exceptions": {
                "by_type": exceptions_by_type,
            },
            "calls_by_status": calls_by_status,
            "timeseries": ts,
            "top_routes": top_routes,
            "top_slowest_routes": top_slowest_routes,
            "issues": issues,
        }


_MONITOR: Optional[ESIMonitor] = None
_MONITOR_LOCK = threading.Lock()


def get_esi_monitor() -> ESIMonitor:
    global _MONITOR
    if _MONITOR is not None:
        return _MONITOR
    with _MONITOR_LOCK:
        if _MONITOR is None:
            _MONITOR = ESIMonitor()
        return _MONITOR
