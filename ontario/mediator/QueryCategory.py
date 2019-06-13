
__author__ = 'Philipp D. Rohde'

from enum import Enum


class QueryCategory(Enum):
    """Enum for query categories."""
    C1 = 'no constant object, single relation'
    C2 = 'no constant object, multiple relations'
    C3 = 'constant object, single relation'
    C4 = 'constant object, multiple relations'
