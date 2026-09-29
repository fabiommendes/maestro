import io
import keyword
import tokenize
from typing import Iterator, Sequence


class Token(str):
    """
    A token in source code.
    """

    __slots__ = ("type", "pos_start")
    type: str
    pos_start: tuple[int, int]

    @property
    def value(self) -> str:
        return str(self)

    @property
    def pos_end(self) -> tuple[int, int]:
        if not self:
            return self.pos_start  # for empty tokens like DEDENT, in python

        line_no, col_no = self.pos_start  # type: ignore
        line_no += (newlines := self.count("\n"))
        if newlines:
            col_no = len(self.rpartition("\n")[-1])
        else:
            col_no += len(self)
        return line_no, col_no

    def __new__(cls, type: str, value: str, pos: tuple[int, int]) -> "Token":
        new = str.__new__(cls, value)
        new.type = type
        new.pos_start = pos
        return new

    def __init__(self, type: str, value: str, pos=None):
        pass

    def __repr__(self):
        return f"{type(self).__name__}({self.type!r}, {self.value!r})"

    def line(self, lines: Sequence[str]) -> str:
        """
        Read the source code lines for the token.
        """
        if self.pos_start is None:
            raise ValueError("No position information")

        i, _ = self.pos_start
        j, _ = self.pos_end  # type: ignore
        if i == j:
            return lines[i]
        return "\n".join(lines[i : j + 1])


# =============================================================================
# Tokenizers
# =============================================================================
PYTHON_CONSTANTS = {"True", "False", "None"}

#
# Python
#
def tokenize_python(src: str) -> Iterator[Token]:
    """
    Tokenize a Python source code.
    """
    fd = io.BytesIO(src.encode("utf-8"))
    tokens = tokenize.tokenize(fd.readline)
    mk_token = _normalize_python_token

    # Skip the first and last tokens.
    try:
        next(tokens)
        tk = mk_token(next(tokens))
        for info in tokens:
            if tk.type != "NL":
                yield tk
            tk = mk_token(info)
    except tokenize.TokenError as ex:
        raise ValueError(str(ex))


def _normalize_python_token(tk: tokenize.TokenInfo) -> "Token":
    data = tk.string
    kind = tokenize.tok_name[tk.exact_type]

    if kind == "NAME":
        if data in PYTHON_CONSTANTS:
            kind = "CONSTANT"
        if keyword.iskeyword(data):
            kind = data.upper()
    (line_no, col_no) = tk.start
    return Token(kind, data, (line_no - 1, col_no))
