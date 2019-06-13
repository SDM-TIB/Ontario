
__author__ = 'Philipp D. Rohde'

from enum import Enum


class PlanType(Enum):
    """Enum for generating different plans."""
    ONTARIO = 'Ontario'  # original optimization of Ontario
    GENERAL_HEURISTICS = 'General Heuristics'  # using general heuristics only
    SOURCE_SPECIFIC_HEURISTICS = 'Source Specific Heuristics'  # add source specific heuristics
