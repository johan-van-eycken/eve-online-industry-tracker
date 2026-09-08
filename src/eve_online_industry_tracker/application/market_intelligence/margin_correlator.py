from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import text

TRITANIUM_TYPE_ID = 34


class MarginCorrelator:
    def __init__(self, *, state: Any, admin_settings: Any = None):
        self._state = state
        self._admin = admin_settings

    def _get_min_days(self) -> int:
        if self._admin:
            try:
                return int(self._admin.get("daily_planner", "planner_mineral_correlation_min_days"))
            except Exception:
                pass
        return 30

    def _get_threshold(self) -> float:
        if self._admin:
            try:
                return float(self._admin.get("daily_planner", "planner_mineral_correlation_threshold"))
            except Exception:
                pass
        return -0.7

    def _is_due(self) -> bool:
        """Returns True if last computation was > 24h ago (or never ran)."""
        from eve_online_industry_tracker.infrastructure.session_provider import StateSessionProvider
        sessions = StateSessionProvider(state=self._state)
        app_session = sessions.app_session()
        try:
            row = app_session.execute(text(
                "SELECT MAX(computed_at) FROM margin_correlation_cache"
            )).fetchone()
            if not row or not row[0]:
                return True
            last = datetime.fromisoformat(str(row[0]))
            return (datetime.utcnow() - last).total_seconds() > 86400
        except Exception:
            return True
        finally:
            try:
                app_session.close()
            except Exception:
                pass

    def run_if_due(self) -> None:
        if not self._is_due():
            return
        try:
            self._run()
        except Exception as e:
            logging.warning("MarginCorrelator failed: %s", e)

    def _run(self) -> None:
        from scipy.stats import pearsonr
        from eve_online_industry_tracker.infrastructure.session_provider import StateSessionProvider
        from eve_online_industry_tracker.infrastructure.models import MarketHistoryModel
        from eve_online_industry_tracker.application.characters.realized_profit import CorporationRealizedProfitLedgerService

        min_days = self._get_min_days()
        threshold = self._get_threshold()
        sessions = StateSessionProvider(state=self._state)
        region_id = 10000002

        # Get Tritanium daily close prices for last 90 days
        cutoff = (datetime.utcnow() - timedelta(days=90)).date().isoformat()
        app_session = sessions.app_session()
        try:
            trit_rows = app_session.query(MarketHistoryModel).filter(
                MarketHistoryModel.type_id == TRITANIUM_TYPE_ID,
                MarketHistoryModel.region_id == region_id,
                MarketHistoryModel.date >= cutoff,
            ).order_by(MarketHistoryModel.date).all()
        finally:
            try:
                app_session.close()
            except Exception:
                pass

        if len(trit_rows) < min_days:
            logging.info("MarginCorrelator: insufficient Tritanium history (%d rows), skipping", len(trit_rows))
            return

        trit_by_date: dict[str, float] = {r.date: float(r.close) for r in trit_rows if r.close}

        # Get realized margin data per type_id from the ledger service
        try:
            realized_svc = CorporationRealizedProfitLedgerService(
                app_session=None,
                sde_session=None,
            )
            # Inject a fresh app session
            app_session2 = sessions.app_session()
            realized_svc._app_session = app_session2
            try:
                rows = realized_svc.list_rows(corporation_id=None)
            finally:
                try:
                    app_session2.close()
                except Exception:
                    pass
        except Exception as e:
            logging.warning("MarginCorrelator: could not load realized profit rows: %s", e)
            return

        # Group margin by (type_id, date)
        from collections import defaultdict
        margins_by_type: dict[int, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
        for row in rows:
            tid = row.get("type_id")
            completed = row.get("completed_at") or row.get("sale_date") or row.get("date")
            margin = row.get("realized_margin_fraction")
            if tid and completed and margin is not None:
                try:
                    date_str = str(completed)[:10]  # ISO date portion
                    margins_by_type[int(tid)][date_str].append(float(margin))
                except Exception:
                    pass

        # Get active plan type_ids for cleanup
        app_session3 = sessions.app_session()
        try:
            active_type_ids = set(
                int(r[0]) for r in app_session3.execute(text(
                    "SELECT DISTINCT bpi.type_id FROM build_plan_item bpi "
                    "JOIN build_plan bp ON bp.id = bpi.plan_id WHERE bp.status = 'active'"
                )).fetchall()
            )
        except Exception:
            active_type_ids = set(margins_by_type.keys())
        finally:
            try:
                app_session3.close()
            except Exception:
                pass

        now_iso = datetime.utcnow().isoformat()

        # Compute Pearson per type_id
        for type_id in active_type_ids:
            try:
                margin_by_date = margins_by_type.get(type_id, {})
                # Find common dates between Tritanium and this item
                common_dates = sorted(set(trit_by_date.keys()) & set(margin_by_date.keys()))
                if len(common_dates) < min_days:
                    continue

                trit_series = [trit_by_date[d] for d in common_dates]
                margin_series = [
                    sum(margin_by_date[d]) / len(margin_by_date[d])
                    for d in common_dates
                ]

                pearson_r, _ = pearsonr(trit_series, margin_series)
                is_squeeze = bool(pearson_r < threshold)

                app_session4 = sessions.app_session()
                try:
                    app_session4.execute(text(
                        "INSERT OR REPLACE INTO margin_correlation_cache "
                        "(type_id, pearson_correlation, is_squeeze_sensitive, data_points, computed_at) "
                        "VALUES (:type_id, :pearson_correlation, :is_squeeze_sensitive, :data_points, :computed_at)"
                    ), {
                        "type_id": type_id,
                        "pearson_correlation": float(pearson_r),
                        "is_squeeze_sensitive": is_squeeze,
                        "data_points": len(common_dates),
                        "computed_at": now_iso,
                    })
                    app_session4.commit()
                finally:
                    try:
                        app_session4.close()
                    except Exception:
                        pass
            except Exception as e:
                logging.warning("MarginCorrelator: failed for type_id=%s: %s", type_id, e)

        # Cleanup: delete rows for type_ids not in active plan
        app_session5 = sessions.app_session()
        try:
            app_session5.execute(text(
                "DELETE FROM margin_correlation_cache WHERE type_id NOT IN "
                f"({','.join(str(t) for t in active_type_ids) or '0'})"
            ))
            app_session5.commit()
        except Exception as e:
            logging.warning("MarginCorrelator: stale cleanup failed: %s", e)
        finally:
            try:
                app_session5.close()
            except Exception:
                pass

        logging.info("MarginCorrelator: updated %d type_ids", len(active_type_ids))
