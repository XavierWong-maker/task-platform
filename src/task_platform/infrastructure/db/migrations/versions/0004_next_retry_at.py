"""job_executions.next_retry_at

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-27
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("job_executions") as batch:
        batch.add_column(sa.Column("next_retry_at", sa.DateTime(), nullable=True))
    op.create_index("ix_job_executions_next_retry_at", "job_executions", ["next_retry_at"])


def downgrade() -> None:
    op.drop_index("ix_job_executions_next_retry_at", table_name="job_executions")
    with op.batch_alter_table("job_executions") as batch:
        batch.drop_column("next_retry_at")
