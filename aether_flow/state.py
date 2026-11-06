"""Event-sourced execution state and run persistence."""

from __future__ import annotations
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from .models import TaskResult, TaskStatus


@dataclass
class WorkflowRun:
    run_id: str
    dag_id: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    results: Dict[str, TaskResult] = field(default_factory=dict)
    context: Dict[str, Any] = field(default_factory=dict)
    events: List[dict] = field(default_factory=list)

    def record_event(self, event_type: str, data: Optional[dict] = None) -> None:
        self.events.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event": event_type,
            "data": data or {},
        })


class StateStore:
    """Thread-safe in-memory store maintaining execution history and event audit trail."""

    def __init__(self):
        self.runs: Dict[str, WorkflowRun] = {}

    def create_run(self, dag_id: str, initial_context: Optional[dict] = None) -> WorkflowRun:
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run = WorkflowRun(
            run_id=run_id,
            dag_id=dag_id,
            status="running",
            started_at=datetime.now(timezone.utc),
            context=dict(initial_context or {}),
        )
        run.record_event("WORKFLOW_STARTED", {"dag_id": dag_id})
        self.runs[run_id] = run
        return run

    def get_run(self, run_id: str) -> Optional[WorkflowRun]:
        return self.runs.get(run_id)

    def list_runs(self) -> List[WorkflowRun]:
        return sorted(self.runs.values(), key=lambda r: r.started_at, reverse=True)
