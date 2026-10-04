"""Silver: the layer that reads Bronze and says what the data currently means.

Bronze answers "what arrived, and in what order". It holds two descriptions of
the same tables that are each incomplete on their own - a bulk snapshot frozen
at one WAL position, and every change that happened after it. Neither is the
current state.

This package reconciles them. See docs/spark.md.
"""
