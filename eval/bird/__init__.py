"""BIRD Mini-Dev in PostgreSQL: data, loader, scorer, label tracks and the evaluation harness.

- :mod:`eval.bird.data`: where every file comes from (pinned by sha256), the
  download cache, the two label tracks and the fixed 50-question subset.
- :mod:`eval.bird.load`: ``make bird``, the eleven databases in PostgreSQL
  (from the official dump, or converted from the SQLite files), a read-only
  role, the catalogs the agent sees and the gold queries verified.
- :mod:`eval.bird.score`: execution accuracy as BIRD computes it, and the
  cross-check against the official scorer.
- :mod:`eval.bird.corrected`: the corrected-label track, transpiled from
  SQLite and verified by execution on both engines.
- :mod:`eval.bird.run`: ``make eval``, the agent on a track, by variant and model.
- :mod:`eval.bird.link_eval`: ``make eval-link``, schema-linking recall and precision.
"""
