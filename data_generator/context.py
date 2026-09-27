"""Shared generation context passed to every entity module."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from data_generator.config import GenerationConfig
from data_generator.naming import NameFactory
from data_generator.reference import ReferenceData
from data_generator.relationships import Ecosystem
from data_generator.rng import RandomStream
from data_generator.timeline import start_of_day


@dataclass(slots=True)
class GeneratorContext:
    config: GenerationConfig
    reference: ReferenceData
    ecosystem: Ecosystem
    names: NameFactory

    @classmethod
    def create(cls, config: GenerationConfig) -> GeneratorContext:
        reference = ReferenceData.load(config.reference_dir)
        return cls(
            config=config,
            reference=reference,
            ecosystem=Ecosystem(reference),
            names=NameFactory(reference),
        )

    def stream(self, *path: object) -> RandomStream:
        """A named random stream rooted at the configured master seed."""
        return RandomStream(self.config.seed, *path)

    @property
    def simulation_end_date(self) -> date:
        return self.config.timeline.simulation_end_date

    @property
    def simulation_end(self) -> datetime:
        return start_of_day(self.config.timeline.simulation_end_date)
