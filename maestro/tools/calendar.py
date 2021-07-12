import datetime
import io
import re
from collections import deque
from typing import Sequence


class ParserError(ValueError):
    """
    Error raised during parsing
    """


class Parser:
    """
    Parse calendar files. The format is exemplified bellow::

        Start: 2020-03-09
        End: 2020-07-06
        Weekdays: Mon, Fri
        Skip:
        - 2020-04-10: *Feriado: Paixão de Cristo (Opcional)*
        - 2020-04-20: *Feriado: Tiradentes*

        ----------------------------------------------------------
        First day

        * Can have multiple lines
        * Another sub-topic
        ----------------------------------------------------------
        Second day

        * Each day is separated by a line of at least 3 dashes.
        ----------------------------------------------------------
        And so on...
    """
    COMMA_SEP = re.compile(r',\s*')
    LINE_SEP = re.compile(r'---+')
    DAYS = {
        'Mon': 0,
        'Tue': 1,
        'Wed': 2,
        'Thu': 3,
        'Fri': 4,
        'Sat': 5,
        'Sun': 6,
    }

    def __init__(self, data):
        head, *blocks = self.LINE_SEP.split(data)
        self.head = io.StringIO(head)
        self.blocks = [io.StringIO(block) for block in blocks]

    def parse(self):
        data = self.parse_head(self.head)
        blocks = [self.parse_block(block) for block in self.blocks]
        return Calendar(data.pop('start'), data.pop('end'), blocks, **data)

    def parse_cte(self, data, expect):
        got = data.read(len(expect))
        if got != expect:
            raise ParserError(f"expect: {expect}, got {got}")
        return got

    def skip_ws(self, data):
        ws = []
        chr = data.read(1)
        while chr and chr.isspace():
            ws.append(chr)
            chr = data.read(1)
        if chr:
            data.seek(data.tell() - 1)
        return ''.join(ws)

    def parse_head(self, data):
        self.skip_ws(data)
        self.parse_cte(data, 'Start:')
        start = self.parse_date(data)

        self.skip_ws(data)
        self.parse_cte(data, 'End:')
        end = self.parse_date(data)

        self.skip_ws(data)
        self.parse_cte(data, 'Weekdays:')
        weekdays = self.parse_weekdays(data)

        self.skip_ws(data)
        self.parse_cte(data, 'Skip:')
        skip = self.parse_skip(data)

        return {'start': start, 'end': end, 'weekdays': weekdays, 'skip': skip}

    def parse_weekdays(self, data):
        days = self.COMMA_SEP.split(data.readline(1024).strip())
        return tuple(map(self.DAYS.__getitem__, days))

    def parse_skip(self, data):
        skip = {}
        self.skip_ws(data)
        while True:
            line = data.readline(1024)
            if self.LINE_SEP.fullmatch(line) or not line:
                break
            elif line.isspace():
                continue
            else:
                date, comment = self.parse_skip_entry(line)
                skip[date] = comment
        return skip

    def parse_skip_entry(self, line):
        date, comment = line.lstrip('- ').split(':', 1)
        date = datetime.date.fromisoformat(date)
        return date, comment.strip()

    def parse_date(self, data):
        self.skip_ws(data)
        year = int(data.read(4))
        self.parse_cte(data, '-')
        month = int(data.read(2))
        self.parse_cte(data, '-')
        day = int(data.read(2))
        return datetime.date(year, month, day)

    def parse_block(self, data):
        data = data.read().strip()
        return '\n'.join(line.rstrip() for line in data.splitlines())


class Calendar:
    """
    Calendar object.
    """

    start: datetime.date
    end: datetime.date
    data: Sequence[str]
    weekdays: Sequence[int]
    skip: Sequence[datetime.date]

    def __init__(self, start, end, data, weekdays=(0, 1, 2, 3, 4), skip=(   )):
        weekdays = tuple(sorted(weekdays))
        while start.weekday() not in weekdays:
            start = start + datetime.timedelta(days=1)
        self.start = start
        self.end = end
        self.data = data
        self.weekdays = weekdays
        self.skip = dict(skip)

        self._width = max(max(map(len, st.splitlines())) for st in self.data)
        self._width = max(10, self._width)
        self._max_day = max(self.weekdays)
        self._week_map = {}
        day, *rest = weekdays
        for next in rest:
            self._week_map[day] = next - day
            day = next
        self._week_map[day] = (self.weekdays[0] - day) % 7

    def __str__(self):
        return self.render_rst_table()

    def _item_lines(self, item, date, week):
        n = self._width + 1
        dd = '%02i' % date.day
        mm = '%02i' % date.month
        week = '   ' if week is None else str(week).rjust(3)
        template_first = f'|  {week}   | {dd}/{mm} | %s|'
        template_rest = '|        |       | %s|'
        first, *rest = item.splitlines()
        return [
            template_first % first.ljust(n),
            *(template_rest % line.ljust(n) for line in rest)
        ]

    def next_date(self, date: datetime.date) -> datetime.date:
        """
        Return next date in calendar from the current (valid) date.
        """
        return date + datetime.timedelta(days=self._week_map[date.weekday()])

    def render_rst_table(self) -> str:
        """
        Render calendar as a ReSTructuredText table.
        """
        line_tail = '-' * (self._width + 2) + '+'
        line_full = '+--------+-------+' + line_tail
        line_inner = '|        +-------+' + line_tail
        lines = [
            line_full,
            '| Semana | Dia   | Atividade%s |' % (' ' * (self._width - 9)),
            line_full.replace('-', '='),
        ]

        date = self.start
        start_weekday = date.weekday()
        items = deque(self.data)

        while items:
            item = items.popleft()

            week = None
            if date.weekday() == start_weekday:
                week = (date - self.start).days // 7 + 1

            if date in self.skip:
                items.appendleft(item)
                item = self.skip[date]
                lines.extend(self._item_lines(item, date, week))
            else:
                lines.extend(self._item_lines(item, date, week))

            lines.append(line_full if date.weekday() == self._max_day else line_inner)
            date = self.next_date(date)

        lines[-1] = line_full
        return '\n'.join(lines)

    def describe(self) -> str:
        """
        Return string with overall description of Calendar object.
        """

        date = self.start
        n = len(self.data) - 1
        while n:
            if date not in self.skip:
                n -= 1
            date = self.next_date(date)

        lines = [
            f'Start date: {self.start.isoformat()}',
            f'Expected End: {self.end.isoformat()}',
            f'Real End: {date.isoformat()}',
        ]
        return '\n'.join(lines)


def parse(src: str) -> Calendar:
    """
    Parse calendar source and return the corresponding Calendar instance.
    """
    parser = Parser(src)
    return parser.parse()
