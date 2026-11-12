"""workflow_runs.parent_run_id / workflow_node_runs.reused

Revision ID: 0006
Revises: 0005
Create Date: 2026-11-11

不用 batch_alter_table：SQLite 的 batch 会重建表，而本项目开启了外键，
重建父表会级联删除子表数据。这里只是简单的加列，直接 ALTER TABLE ADD COLUMN。
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("workflow_runs", sa.Column("parent_run_id", sa.String(32), nullable=True))
    op.add_column(
        "workflow_node_runs",
        sa.Column("reused", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("workflow_node_runs", "reused")
    op.drop_column("workflow_runs", "parent_run_id")
