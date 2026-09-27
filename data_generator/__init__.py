"""Relationship-aware synthetic data generator for the AdTech platform.

The generator builds one coherent advertising ecosystem rather than a set of
independent tables: every foreign key is taken from a parent object that has
already been generated, and every event is derived from the concrete entities
that produced it.
"""

__version__ = "0.1.0"
