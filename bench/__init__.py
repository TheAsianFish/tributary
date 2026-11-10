"""Ingest benchmarks: fault injection (``make bench``) and scaling (``make bench-scale``).

Everything here drives the Compose stack from the host and makes no model calls.
The pure parts (event generation, the sustained-rate rule, quantiles, the
attribution arithmetic, report rendering) are unit-tested offline; the drivers
need Docker and write their raw output under ``bench/results/``.
"""
