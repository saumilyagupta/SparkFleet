"""Local web dashboard for clusterctl (`python -m clusterctl dashboard`).

The dashboard is a thin HTTP layer over the same functions the CLI calls;
it never reimplements cluster operations.
"""
