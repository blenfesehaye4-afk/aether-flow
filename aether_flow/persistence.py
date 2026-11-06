"""SQLite-backed persistent state store for durable workflow execution and checkpointing."""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .models import TaskResult, TaskStatus
from .state import StateStore, WorkflowRun


class SQLiteStateStore(StateStore):
    """ACID-durable SQLite state store for workflow runs, task checkpoints, and event logs."""

    def __init__(self, db_path: Union[str, Path] = "aether_state.db"):
        super().__init__()
        self.db_path = Path(db_path)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS workflow_runs (
                    run_id TEXT PRIMARY KEY,
                    dag_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    context_json TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS task_checkpoints (
                    run_id TEXT NOT NULL,
                    task_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    output_json TEXT,
                    error TEXT,
                    duration REAL,
                    attempt INTEGER,
                    started_at TEXT,
                    completed_at TEXT,
                    PRIMARY KEY (run_id, task_id),
                    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS run_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    data_json TEXT NOT NULL,
                    FOREIGN KEY (run_id) REFERENCES workflow_runs(run_id) ON DELETE CASCADE
                );
            """)

    def save_run(self, run: WorkflowRun) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                INSERT INTO workflow_runs (run_id, dag_id, status, started_at, completed_at, context_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    status=excluded.status,
                    completed_at=excluded.completed_at,
                    context_json=excluded.context_json
                """,
                (
                    run.run_id,
                    run.dag_id,
                    run.status,
                    run.started_at.isoformat(),
                    run.completed_at.isoformat() if run.completed_at else None,
                    json.dumps(run.context, default=str),
                ),
            )

            # Save task results as checkpoints
            for task_id, res in run.results.items():
                conn.execute(
                    """
                    INSERT INTO task_checkpoints (run_id, task_id, status, output_json, error, duration, attempt, started_at, completed_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, task_id) DO UPDATE SET
                        status=excluded.status,
                        output_json=excluded.output_json,
                        error=excluded.error,
                        duration=excluded.duration,
                        attempt=excluded.attempt,
                        completed_at=excluded.completed_at
                    """,
                    (
                        run.run_id,
                        task_id,
                        res.status.value,
                        json.dumps(res.output, default=str) if res.output is not None else None,
                        res.error,
                        res.duration,
                        res.attempt,
                        res.started_at.isoformat() if res.started_at else None,
                        res.completed_at.isoformat() if res.completed_at else None,
                    ),
                )

    def load_run(self, run_id: str) -> Optional[WorkflowRun]:
        with self._get_connection() as conn:
            row = conn.execute("SELECT * FROM workflow_runs WHERE run_id = ?", (run_id,)).fetchone()
            if not row:
                return None

            started_at = datetime.fromisoformat(row["started_at"])
            completed_at = datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None
            context = json.loads(row["context_json"])

            run = WorkflowRun(
                run_id=row["run_id"],
                dag_id=row["dag_id"],
                status=row["status"],
                started_at=started_at,
                completed_at=completed_at,
                context=context,
            )

            # Load checkpoints
            tasks_cursor = conn.execute("SELECT * FROM task_checkpoints WHERE run_id = ?", (run_id,))
            for t_row in tasks_cursor:
                task_id = t_row["task_id"]
                output = json.loads(t_row["output_json"]) if t_row["output_json"] is not None else None
                res = TaskResult(
                    task_id=task_id,
                    status=TaskStatus(t_row["status"]),
                    output=output,
                    error=t_row["error"],
                    duration=t_row["duration"] or 0.0,
                    attempt=t_row["attempt"] or 1,
                )
                run.results[task_id] = res

            return run
