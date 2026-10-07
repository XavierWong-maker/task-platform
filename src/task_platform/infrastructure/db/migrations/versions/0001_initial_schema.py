"""initial schema: jobs / schedules / job_executions / job_logs

Revision ID: 0001
Revises:
Create Date: 2026-10-06
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "jobs",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("name", sa.String(128), nullable=False),
        sa.Column("job_type", sa.String(32), nullable=False),
        sa.Column("handler", sa.String(128), nullable=False),
        sa.Column("timeout_seconds", sa.Float(), nullable=False),
        sa.Column("retry_max_attempts", sa.Integer(), nullable=False),
        sa.Column("retry_backoff", sa.String(16), nullable=False),
        sa.Column("retry_base_delay_seconds", sa.Float(), nullable=False),
        sa.Column("retry_max_delay_seconds", sa.Float(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("name"),
    )

    op.create_table(
        "schedules",
        sa.Column("id", sa.String(32), primary_key=True),
        sa.Column("job_id", sa.String(32), nullable=False),
        sa.Column("schedule_type", sa.String(16), nullable=False),
        sa.Column("interval_seconds", sa.Float(), nullable=True),
        sa.Column("time_of_day", sa.Time(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
    )
    op.create_index("ix_schedules_job_id", "schedules", ["job_id"])

    op.create_table(
        "job_executions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(32), nullable=False),
        sa.Column("job_id", sa.String(32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("timeout_seconds", sa.Float(), nullable=True),
        sa.Column("max_attempts", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["jobs.id"], ondelete="CASCADE"),
        sa.UniqueConstraint("run_id", "attempt", name="uq_job_executions_run_attempt"),
    )
    op.create_index("ix_job_executions_job_id_status", "job_executions", ["job_id", "status"])

    op.create_table(
        "job_logs",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(32), nullable=False),
        sa.Column("attempt", sa.Integer(), nullable=False),
        sa.Column("level", sa.String(16), nullable=False),
        sa.Column("message", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["run_id", "attempt"],
            ["job_executions.run_id", "job_executions.attempt"],
            ondelete="CASCADE",
        ),
    )
    op.create_index("ix_job_logs_run_id_attempt", "job_logs", ["run_id", "attempt"])


def downgrade() -> None:
    op.drop_table("job_logs")
    op.drop_table("job_executions")
    op.drop_table("schedules")
    op.drop_table("jobs")
