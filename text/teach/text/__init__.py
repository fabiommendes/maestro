"""
Manipulate programming exercises based on line permutations.
"""
__version__ = "0.1"
__author__ = "Fábio Macêdo Mendes"
__email__ = "fabiomacedomendes@gmail.com"
__license__ = "MIT"

from .io import IoQuestion, TextIoQuestion, NumericIoQuestion, WordsIoQuestion
from .pattern import (
    PatternQuestion,
    TextPatternQuestion,
    NumericPatternQuestion,
    WordsPatternQuestion,
    AllPatternQuestion,
    AnyPatternQuestion,
)
