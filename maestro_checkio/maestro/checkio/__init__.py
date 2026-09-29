"""
Fetch classroom information from the Checkio.org website.
"""
from .api import Group, Progress, groups, questions, question_dataframe

__version__ = "0.1"
__author__ = "Fábio Macêdo Mendes"
__email__ = "fabiomacedomendes@gmail.com"
__all__ = ["Group", "Progress", "groups", "questions", "question_dataframe"]
