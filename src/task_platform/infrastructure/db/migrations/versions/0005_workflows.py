"""workflows / workflow_runs / workflow_node_runs

Revision ID: 0005
Revises: 0004
Create Date: 2026-11-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "workflows",
        sa.Column("name", sa.String(128), primary_key=True),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )

    op.create_table(
        "workflow_runs",
        sa.Column("run_id", sa.String(32), primary_key=True),
        sa.Column("workflow_name", sa.String(128), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("definition", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["workflow_name"], ["workflows.name"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_workflow_runs_workflow_name_started_at",
        "workflow_runs",
        ["workflow_name", "started_at"],
    )

    op.create_table(
        "workflow_node_runs",
        sa.Column("workflow_run_id", sa.String(32), nullable=False),
        sa.Column("node_name", sa.String(128), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("job_run_id", sa.String(32), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("workflow_run_id", "node_name"),
        sa.ForeignKeyConstraint(["workflow_run_id"], ["workflow_runs.run_id"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("workflow_node_runs")
    op.drop_index("ix_workflow_runs_workflow_name_started_at", table_name="workflow_runs")
    op.drop_table("workflow_runs")
    op.drop_table("workflows")
