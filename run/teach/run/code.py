from functools import lru_cache
import io
import re
from typing import Iterable, Iterator, Generic, TypeVar
import zlib

from teach.core import Feedback
from teach.core.string import split_indentation

from .lang import Lang
from .tokenize import Token, tokenize_python

#: The source code type, i.e., str in most cases.
Src = TypeVar("Src")

TRANSFORMS: dict[Lang, type["CodeTransform"]] = {}
register_transform = lambda lang: lambda cls: TRANSFORMS.setdefault(lang, cls) and cls

CHECK_DOC = """Args:
            src: The source code.

        Return:
            The corresponding user feedback.
        """


class CodeTransform(Generic[Src]):
    """
    Represents a programming language and provides an uniform API
    to instrospect source code written in that language.
    """

    lang: Lang
    line_comment: str
    open_block_comment: str
    close_block_comment: str
    indent_level: str

    #
    # Extract useful information from the source.
    #
    def extract_names(self, src: Src) -> set[str]:
        """
        Extract all variable and function names from the source.
        """
        names = set()
        for token in self.tokens(src):
            if token.type == "NAME":
                names.add(str(token))
        return names

    def extract_imports(self, src: Src) -> set[str]:
        """
        Extract all imported modules from the source.
        """
        raise NotImplementedError

    def tokens(self, src: Src, keep_comments: bool = False) -> Iterable["Token"]:
        """
        Extract all tokens from the source.
        """
        if keep_comments:
            yield from self._tokens(src)
        else:
            for tk in self._tokens(src):
                if tk.type != "COMMENT":
                    yield tk

    def _tokens(self, src: Src) -> Iterator["Token"]:
        raise NotImplementedError

    def lines(self, src: Src) -> Iterator[str]:
        """
        Return an iterator over the source lines of code.
        """
        raise NotImplementedError

    def clean_comments(self, src: Src) -> Src:
        """
        Remove comments from the source.
        """
        raise NotImplementedError

    def split_comment_header(self, src: Src) -> tuple[str, str]:
        """
        Split a source file into a header and a body.

        The header is typically a starting block comment and the body is the source
        code that follows.
        """
        raise NotImplementedError

    def render_line_comment(self, line: str, prefix: str = "") -> str:
        """
        Render a line comment.
        """
        return f"{self.line_comment}{prefix} {line}"

    def render_block_comment(
        self, comment: str, open=None, close=None, prefix=""
    ) -> str:
        """
        Render a block comment.
        """
        open = open or self.open_block_comment
        close = close or self.close_block_comment
        lines = comment.splitlines(keepends=True)

        if len(lines) <= 1:
            return "".join([open, *lines, close])

        line_it = iter(lines)
        min_indent, first = split_indentation(next(line_it))
        parts = ["", open, "\n", min_indent, prefix, first]
        indent = min_indent

        for line in line_it:
            indent, line = split_indentation(line)
            if len(indent) < len(min_indent) and line:
                min_indent = indent
            parts.extend([indent, prefix, line])

        parts[0] = min_indent
        parts.extend(["\n", min_indent, close])
        return "".join(parts)

    #
    # Impose restrictions to source code.
    #
    def verify_restrictions(
        self,
        src: Src,
        forbid_keywords: Iterable[str] | None = None,
        forbid_names: Iterable[str] | None = None,
        forbid_imports: Iterable[str] | None = None,
        allow_imports: Iterable[str] | None = None,
    ) -> Feedback:
        """
        Verify the source against the given restrictions.
        """
        feedbacks = [self.check_syntax(src)]

        if forbid_keywords is not None:
            out = self.forbid_syntax(src, forbid_keywords)
            feedbacks.append(out)
        if forbid_names is not None:
            out = self.forbid_names(src, forbid_names)
            feedbacks.append(out)
        if forbid_imports is not None:
            out = self.forbid_imports(src, forbid_imports)
            feedbacks.append(out)
        if allow_imports is not None:
            out = self.forbid_imports(src, allow_imports)
            feedbacks.append(out)

        return Feedback.new(*feedbacks)

    def check_syntax(self, src: Src) -> Feedback:
        f"""
        Check if the source syntax is valid.

        {CHECK_DOC}
        """
        raise NotImplementedError

    def forbid_syntax(self, src: Src, keywords: Iterable[str]) -> Feedback:
        f"""
        Check if the source contains the given keywords or syntatic elements.

        {CHECK_DOC}
        """
        keywords = set(keywords)
        for token in self.tokens(src):
            if (tk := str(token)) in keywords:
                return Feedback.Fail("forbidden-syntax", f"{tk} is not allowed.")
        return Feedback.Success()

    def forbid_names(self, src: Src, names: Iterable[str]) -> Feedback:
        f"""
        Check if the source uses the given names.

        It does not distinguishes between variables and functions.

        {CHECK_DOC}
        """
        bad_names = self.extract_names(src).intersection(names)
        return self._bad_elements("names", bad_names)

    def forbid_imports(self, src: Src, names: Iterable[str]) -> Feedback:
        """
        Check if the source imports any one of the given modules.
        """
        bad_imports = self.extract_imports(src).intersection(names)
        return self._bad_elements("modules", bad_imports)

    def allow_imports(self, src: Src, names: Iterable[str]) -> Feedback:
        f"""
        Allow the source to import any one of the given modules.

        {CHECK_DOC}
        """
        bad_imports = set(names) - self.extract_imports(src)
        return self._bad_elements("modules", bad_imports)

    def _bad_elements(self, name: str, bad: set[str]) -> Feedback:
        if not bad:
            return Feedback.Success()
        msg = ", ".join(bad)
        msg = f"{msg} {name} are not allowed."
        return Feedback.Fail(name.replace(" ", "-"), msg)

    #
    # Source code complexity metrics.
    #
    def count_tokens(self, src: Src) -> int:
        """
        Return the number of tokens in the source.

        This is a crude estimate of code complexity.
        """
        return sum(1 for _ in self.tokens(src))

    def count_loc(self, src: Src) -> int:
        """
        Count the number of physical lines of code.

        This is a crude estimate of code complexity.
        """
        src = self.clean_comments(src)
        return sum(1 for ln in self.lines(src) if ln.strip())

    def count_statements(self, src: Src) -> int:
        """
        Count the number of statements in the source.

        This is a crude estimate of code complexity.
        """
        raise NotImplementedError

    def count_zip_complexity(self, src: Src) -> int:
        """
        Count the number bytes in a zipped version of code.

        This is a crude estimate of code complexity.
        """
        raise NotImplementedError

    def render_tokens(self, tokens: Iterable["Token"]) -> str:
        """
        Render a sequence of tokens.
        """
        data = io.StringIO()
        line_no, col_no = 0, 0
        for token in tokens:
            if token.pos_start is None:
                raise ValueError("Token position is not set.")

            i, j = token.pos_start
            if i == line_no:
                data.write(" " * (j - col_no))
            else:
                data.write("\n" * (i - line_no))
                data.write(" " * j)
            data.write(token.value)

            if token.pos_end is None:
                raise ValueError("Token end position is not set.")
            line_no, col_no = token.pos_end

        return data.getvalue()

    def indent_line(self, line: str, level: int | str = 1) -> str:
        """
        Indent line by the given indentation level.
        """
        if isinstance(level, str):
            return level + line
        return (self.indent_level * level) + line

    def is_line_comment(self, line: str) -> bool:
        """
        Check if the line is a line comment.
        """
        return line.lstrip().startswith(self.line_comment)

    def extract_line_comment_data(self, line: str) -> str:
        """
        Extract comment string from a line comment.
        """
        try:
            _, data = line.lstrip().split(self.line_comment, 1)
        except IndexError:
            raise ValueError("line is not a comment!")
        return data.removeprefix(" ")


class StrCodeTransform(CodeTransform[str]):
    """
    Code transform for string sources.
    """

    indent_level: str = "\t"

    def count_zip_complexity(self, src: str) -> int:
        data = src.encode("utf-8")
        return len(zlib.compress(data, -1))

    def lines(self, src: str) -> Iterator[str]:
        yield from src.splitlines(keepends=True)

    def split_comment_header(self, src: str) -> tuple[str, str]:
        pre, sep, src = src.partition(self.open_block_comment)
        if sep:
            header, sep, src = src.partition(self.close_block_comment)
        else:
            header = ""
            pre, src = "", pre

        return header.strip(), (pre + src).strip()

    def clean_comments(self, src: str) -> str:
        lines = [*self.lines(src)]
        if self.line_comment is not None:
            self._clean_line_comments(lines)
        if self.open_block_comment is not None:
            self._clean_block_comments(lines)
        return "".join(lines)

    def _clean_line_comments(self, lines: list[str]):
        for i, line in enumerate(lines):
            line, has_comment, _ = line.partition(self.line_comment)
            if has_comment:
                lines[i] = line

    def _clean_block_comments(self, lines: list[str]):
        parts = []
        lines.reverse()
        reading_source = False

        while lines:
            line = lines.pop()

            if reading_source:
                line, sep, rest = line.partition(self.open_block_comment)
                parts.append(line)
                reading_source = not sep
                if sep:
                    lines.append(rest)
            else:
                line, sep, rest = line.partition(self.close_block_comment)
                reading_source = bool(sep)
                if sep:
                    lines.append(rest)
        lines.extend(parts)


@register_transform("python")
class PythonCodeTransform(StrCodeTransform):
    """
    Transform Python code.
    """

    CODING_RE = re.compile(r"\#\s*-\*-\*(en)?coding[:=]\s*([-\w.]+)\s*-\*-")
    lang: Lang = "python"
    line_comment: str = "#"
    open_block_comment: str = ""
    close_block_comment: str = ""
    indent_level: str = "    "

    def check_syntax(self, src: str) -> Feedback:
        try:
            compile(src, "<input>", "exec")
        except SyntaxError as exc:
            return Feedback.Fail("syntax", str(exc))
        else:
            return Feedback.Success()

    def split_comment_header(self, src: str) -> tuple[str, str]:
        comments: list[str] = []
        tokens = iter(self.tokens(src, keep_comments=True))

        # Clean comments, and whitespace
        line_no, col_no = (0, 0)
        while True:
            try:
                tk = next(tokens)
            except StopIteration:
                break

            # Invalid source code. Stop here.
            except ValueError:
                return "", src

            if tk.type == "COMMENT":
                if self.CODING_RE.match(tk.value):
                    continue
                comments.append(tk.value[1:].removeprefix(" "))
                line_no, col_no = tk.pos_end[0] + 1, 0
            elif tk.type in ("NEWLINE", "INDENT", "DEDENT", "WS"):
                continue
            elif tk.type == "STRING":
                msg: str = eval(tk.value).strip()
                comments.extend(msg.splitlines())
                line_no, col_no = tk.pos_end[0] + 1, 0
                break
            else:
                line_no, col_no = tk.pos_start
                break

        lines = src.splitlines()[line_no:]
        if col_no != 0:
            lines[0] = " " * col_no + lines[0]

        return "\n".join(comments), "\n".join(lines).lstrip("\n")

    def render_block_comment(self, comment: str, **kwargs) -> str:  # type: ignore
        kwargs.setdefault("open", '"""')
        kwargs.setdefault("close", '"""')
        return super().render_block_comment(comment, **kwargs)

    def _tokens(self, src: str) -> Iterator["Token"]:
        return tokenize_python(src)


@lru_cache(16)
def get_transform_class(lang: Lang) -> type[CodeTransform]:
    """
    Get the code transform class for the given language.
    """
    try:
        return TRANSFORMS[lang]
    except KeyError:
        raise ValueError(f"No code transform for language {lang}")
