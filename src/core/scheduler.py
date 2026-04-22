from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from audit.app_logger import get_logger
from core.settings import Settings

logger = get_logger(__name__)


@dataclass(slots=True)
class SchedulerManager:
    settings: Settings
    scheduler: BackgroundScheduler | None = None

    def start(self, poll_job: Callable[[], None], final_job: Callable[[], None], update_job: Callable[[], None]) -> None:
        if self.scheduler is not None:
            return

        scheduler = BackgroundScheduler(timezone=self.settings.app.timezone)

        for hhmm in self.settings.app.polling_times:
            hour, minute = hhmm.split(":")
            scheduler.add_job(
                poll_job,
                trigger=CronTrigger(day_of_week="mon-fri", hour=int(hour), minute=int(minute)),
                id=f"poll_{hhmm}",
                replace_existing=True,
            )

        final_h, final_m = self.settings.app.final_signal_time.split(":")
        scheduler.add_job(
            final_job,
            trigger=CronTrigger(day_of_week="mon-fri", hour=int(final_h), minute=int(final_m)),
            id="final_signal",
            replace_existing=True,
        )

        upd_h, upd_m = self.settings.app.update_time_after_close.split(":")
        scheduler.add_job(
            update_job,
            trigger=CronTrigger(day_of_week="mon-fri", hour=int(upd_h), minute=int(upd_m)),
            id="daily_update",
            replace_existing=True,
        )

        scheduler.start()
        self.scheduler = scheduler
        logger.info("Scheduler started with %s jobs", len(scheduler.get_jobs()))

    def shutdown(self) -> None:
        if self.scheduler is None:
            return
        self.scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped at %s", datetime.now().isoformat())
        self.scheduler = None

    def jobs_snapshot(self) -> list[dict[str, str]]:
        if self.scheduler is None:
            return []
        out: list[dict[str, str]] = []
        for job in self.scheduler.get_jobs():
            out.append({"id": job.id, "next_run": str(job.next_run_time)})
        return out
