"""Builder costs refresh pipeline.

Reads ``build_watchlist`` (the canonical, buildable item list in
``buildcost.db``), fetches manufacturing costs from EverRef, and persists
them to ``buildcost.db``. The watchlist is its own source of truth — managed
via ``build-watchlist`` and the auto-mirror in ``add_watchlist``.
Market-independent — no ``MarketContext`` required.

Public entry point: :func:`mkts_backend.builder_costs.runner.run`.
"""
