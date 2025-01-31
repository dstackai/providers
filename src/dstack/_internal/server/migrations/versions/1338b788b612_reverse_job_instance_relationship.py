"""Reverse Job-Instance relationship

Revision ID: 1338b788b612
Revises: da574e93fee0
Create Date: 2025-01-16 14:59:19.113534

"""

import sqlalchemy as sa
import sqlalchemy_utils
from alembic import op

# revision identifiers, used by Alembic.
revision = "1338b788b612"
down_revision = "da574e93fee0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("jobs", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "instance_id", sqlalchemy_utils.types.uuid.UUIDType(binary=False), nullable=True
            )
        )
        batch_op.create_foreign_key(
            batch_op.f("fk_jobs_instance_id_instances"),
            "instances",
            ["instance_id"],
            ["id"],
            ondelete="CASCADE",
        )

    op.execute("""
        UPDATE jobs AS j
        SET instance_id = (
            SELECT i.id
            FROM instances AS i
            WHERE i.job_id = j.id
        )
    """)

    with op.batch_alter_table("instances", schema=None) as batch_op:
        batch_op.drop_constraint("fk_instances_job_id_jobs", type_="foreignkey")
        batch_op.drop_column("job_id")


def downgrade() -> None:
    with op.batch_alter_table("instances", schema=None) as batch_op:
        batch_op.add_column(sa.Column("job_id", sa.UUID(), autoincrement=False, nullable=True))
        batch_op.create_foreign_key("fk_instances_job_id_jobs", "jobs", ["job_id"], ["id"])

    # This migration is not fully reversible - we cannot assign multiple jobs to a single instance,
    # thus LIMIT 1
    op.execute("""
        UPDATE instances AS i
        SET job_id = (
            SELECT j.id
            FROM jobs j
            WHERE j.instance_id = i.id
            ORDER by j.submitted_at DESC
            LIMIT 1
        )
    """)

    with op.batch_alter_table("jobs", schema=None) as batch_op:
        batch_op.drop_constraint(batch_op.f("fk_jobs_instance_id_instances"), type_="foreignkey")
        batch_op.drop_column("instance_id")
