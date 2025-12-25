"""
Synthesis module for generating synthetic tax data.

Uses normalizing flows to learn the joint distribution of tax variables
conditioned on demographics from PUF, then generates tax variables
for CPS demographics.
"""

from .transforms import (
    ZeroInflatedTransform,
    LogTransform,
    Standardizer,
    TaxVariableTransformer,
    MultiVariableTransformer,
)
from .flows import ConditionalMAF, MADE, AffineCouplingLayer
from .discrete import (
    BinaryVariableModel,
    CategoricalVariableModel,
    DiscreteVariableSampler,
)
from .synthesizer import TaxSynthesizer

__all__ = [
    # Transforms
    "ZeroInflatedTransform",
    "LogTransform",
    "Standardizer",
    "TaxVariableTransformer",
    "MultiVariableTransformer",
    # Flows
    "ConditionalMAF",
    "MADE",
    "AffineCouplingLayer",
    # Discrete
    "BinaryVariableModel",
    "CategoricalVariableModel",
    "DiscreteVariableSampler",
    # Main class
    "TaxSynthesizer",
]
