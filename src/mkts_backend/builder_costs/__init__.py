"""Builder costs refresh pipeline.

Reads the watchlist from a market DB, filters to manufacturable items via
the SDE, fetches manufacturing costs from EverRef, and persists them to
``buildcost.db``. Market-independent — no ``MarketContext`` required.

Public entry point: :func:`mkts_backend.builder_costs.runner.run`.
"""
