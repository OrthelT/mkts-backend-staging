"""Shared schema for the per-database ``updatelog`` ledger.

Both ``buildcost.db`` and the wcmktprod market DB carry an ``updatelog`` table
that the wcmkts_new frontend probes with
``MAX(timestamp) WHERE table_name=<name>`` to decide whether to trigger a sync.
The two tables must have identical column shape; this mixin is the single
source of truth so the schemas cannot drift independently. Each declarative
``Base`` gets a thin subclass because each base is bound to a different
physical database — but column definitions are owned here.
"""

from __future__ import annotations

from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint


class UpdateLogMixin:
    __tablename__ = "updatelog"
    __table_args__ = (
        UniqueConstraint("table_name", name="uq_updatelog_table_name"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    table_name = Column(String, nullable=False)
    timestamp = Column(DateTime, nullable=False)
