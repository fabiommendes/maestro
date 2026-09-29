"""
Manipulate programming exercises based on line permutations.
"""
__version__ = "0.1"
__author__ = "Fábio Macêdo Mendes"
__email__ = "fabiomacedomendes@gmail.com"
__license__ = "MIT"

from .builder import build
from .error import Error, WrongTypeError
from .feedback import Feedback
from .models import Model, ParentMixin
from .typechecker import typecheck
from .normalizer import normalize
from .describable import Describable, render_description, render_details, print_details
from .assessment import Assessment, Grades, CompetencyId, Grade
from .events import Events, MessageHandler, get_messages
from . import utils
from . import string
from . import fred
