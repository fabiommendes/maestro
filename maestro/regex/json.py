"""
JSON (http://json.org/) is a very popular data serialization format designed to 
be a data-driven subset of Javascript. The website "json.org" has nice diagrams
explaining the details of the format. In this exercise we are going to 
implement the regular expressions necessary to create a JSON lexer.
"""


class Open_brace:
    """
    From the spec:

       "An object is an unordered set of name/value pairs. An object begins 
        with { (left brace) and ends with } (right brace). Each name is followed 
        by : (colon) and the name/value pairs are separated by , (comma)."

    Let us start easy: write a regular expression that matches only the opening
    brace.
    """

    regex = r'\{'


class Control_chars:
    """
    Now we want an expression that matches any other control character as 
    described in the full spec. 

    Read the description of the format on the website in order to identify all 
    control characters.
    """

    regex = r'[:[{},\]]'
    max_length = len(regex)


class Keywords:
    """
    JSON also have a few keywords representing booleans and nulls. Match them!
    """

    regex = r'true|false|null'
    max_length = len(regex)
    accept = 'true false null'
    reject = 'True False Null None'


class Integer:
    """
    Now things start to get complicated...

    Lets crack the number spec: you must create a regex that matches anything 
    before the dot. Ignore scientific notation for now.  
    """

    regex = r'\-?(0|[1-9][0-9]*)'
    max_length = 2 * len(regex)
    accept = '0 1 2 42 -1'
    reject = '01 3.14'


class Simple_float:
    """
    Add a suffix to the previous regex that matches a number with an optional 
    decimal part.
    """

    regex = r'\-?(0|[1-9][0-9]*)(\.[0-9]+)?'
    max_length = 2 * len(regex)
    accept = '42 3.14'
    reject = '01 3a14 3. .42'


class Number:
    """
    Finally, include the optional exponent for scientific notation.

    If you haven't noticed yet, it is possible to construct regular expressions
    by combining strings of simpler regular expressions. You can declare it
    like:

        regex = Simple_float.regex + r'(scientific notation regex)' 
    """
    regex = Simple_float.regex + r'([eE][+-]?[0-9]+)?'
    max_length = 2 * len(regex)
    accept = '42 3.14'
    reject = '01 3a14 3. .42'


class Simple_string:
    """
    We start by matching a very simple string that do not contain any special
    characters.

    It must be enclosed by double quotes and can include any character except
    \ (backslash) or " (double quote) 
    """
    regex = r'"[^\\"]*"'
    accept = ['"hello"', '""', '"hello world!"']
    reject = ['"\n"', '"bad"string"']


class String_with_escaped_double_quote:
    """
    Add support for escaping a inner double quote as in this ugly 
    example: "hello \"World\"!"
    """

    regex = r'"([^\\"]*(\\")?)+"'
    accept = ['"hello"', '""', '"hello world!"']
    reject = ['"\n"', '"bad"string"']


class Full_String:
    """
    Add support for special escaping sequences specified on the spec.
    """

    regex = r'"([^\\"\n]*(\\([bfnrt"/\]|u[0-9a-fA-F]{4}))?)+"'
    accept = ['"hello"', '""', '"hello world!"']
    reject = ['"\n"', '"bad"string"']


#
# HJSON
#
class Line_comments:
    """
    HJSON extends JSON by (among other things) allowing comments. It accepts
    either C-style or hash-style line comments.
    """

    regex = r'(//|#)[^\n]*'
    accept = ['// a C-like comment', '# a Python like comment']
    reject = ['# trailing newlines are not part of the comment\n']


class Block_comments:
    """
    HJSON also accepts C-style block comments. Match them!
    """

    regex = r'/\*([^*]*[^/])*\*/'
    accept = ['/* block comment */']
    reject = ['// a C-like comment', '# a Python like comment']


#
# TOML
#
class Multi_line_bare_strings:
    """
    Just like Python, TOML accept multi-line string literals. Those literals can
    contain new lines and are enclosed in 3 single quotes.
    """
    regex = r"'''([^']*'?)*'''"


class Bare_strings:
    """
    TOML has the concept of bare strings. They work similarly to regular 
    strings, but they do not support escaping. Backslashes are not treated as
    control characters, but rather as literal backslashes.

    Bare strings are enclosed in single quotes and cannot have newlines or 
    the line feed character. 
    """
    regex = r"'[^'\n\r]'"


class Strings:
    """
    Let us start with strings. In TOML, string literals follow basically the 
    same rules as JSON, but they also allow for unicode code points with 8 
    digits to be escaped with an uppercase U.
    """

    regex = r'"([^\\"\n\r]*(\\([bfnrt"/\]|u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}))?)+"'
    accept = [r'"unicode: \U02468AD"']
    reject = [r'"broken unicode: \U02468"']


class Multi_line_strings:
    """
    Standard multi-line strings are enclosed in triple quotes and accept 
    escaping just like regular strings. The only difference between regular
    strings and multi line strings is the the later can contain line breaks 
    and are enclosed by 3 double quotes, instead of one.

    Create a regex that match a multi line string.
    """

    regex = r'"([^\\"]*(\\([bfnrt"/\]|u[0-9a-fA-F]{4}|U[0-9a-fA-F]{8}))?)+"'
    accept = [r'"unicode: \U02468AD"']
    reject = [r'"broken unicode: \U02468"']


class Integer:
    """
    In TOML, integers can be prefixed with a sign and can contain underscores
    in order to group digits in a human-friendly way.
    """

    regex = r'[-+]?(0|[1-9](_?[0-9]+)*'
    accept = '0 1 2 42 -42 +42 1_234 1_23_45_678'
    reject = '01 _ _1 2_ ++1 A42 1__2'


class Binary:
    """
    Binary literals start with 0b and can have one or binary digits (0 or 1).

    Underscores are allowed.
    """

    regex = r'[-+]?0b[01](_[01]+)*'
    accept = '0b11 +0b10 -0b01 0b1010_1101'


class Octal:
    """
    Octal follow similar rules of binary literals, but uses the 0o prefix and
    (obviously) accept any octal digits.
    """

    regex = r'[-+]?0o[0-8](_[0-8]+)*'
    accept = '0o42 +0b12_34'


class Hexadecimal:
    """
    Hexadecimal literals start with 0x and can have one or more hexadecimal
    digits. Hexadecimal digits can be lowercase or uppercase.
    """

    regex = r'[-+]?0x[0-9a-fA-F](_[0-9a-fA-F]+)*'
    accept = '0x42 0xff 0xFF 0x123_abc'


class Decimal:
    """
    Floats add a decimal part and a scientific notation to integers.

    Implement a regex that adds a required decimal part that follows similar
    rules as integers.
    """
    regex = r'[-+]?(0|[1-9](_?[0-9]+)*\.([0-9](_?[0-9]+)*'


class Floats:
    """
    Now implement the optional exponent part.
    """
    regex = Decimal.regex + r'([Ee][-+]?[1-9](_?[0-9]+)*)?'


class Float_literals:
    """
    TOML understands a few floating point literals for special floating point
    values. It understands inf and nan in signed and unsigned forms.
    """

    regex = r'[+-]?(inf|nan)'


class Date:
    """
    TOML accept RFC 3339 datetime values. Unfortunately the whole spec is too
    complicated to be implemented with regular expressions.

    Let us start with a pattern to match only simple dates in the YYYY-MM-DD
    format. 

    Although it is feasible, it is not considered to be a good practice to 
    validate the days and values using the regex itself.
    """
    regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'


class Time:
    """
    Time is given in the hh:mm:ss or hh:mm:ss.ssssss formats.
    """
    regex = r'[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{6})'


class Datetime:
    """
    A full datetime format can be given by joining the date and time parts 
    either with an uppercase T or an space.
    """
    regex = Date.regex + r'[T ]' + Time.regex


class Full_datetime:
    """
    The full format involves a datetime + a timezone specification.

    The timezone can be given as an offset in the format (+|-)hh:mm or as the
    literal Z letter to specify UTC.  
    """
    regex = Datetime.regex + r'(Z|[0-9]{2}:[0-9]{2})'



# JSON parsers
import ox


lexer = ox.make_lexer([
    ('LBRACE', r'\{'),
    ('RBRACE', r'\}'),
    ('LBRACKET', r'\['),
    ('RBRACKET', r'\]'),
    ('COMMA', r','),
    ('COLON', r':'),
    ('STRING', Full_String.regex),
    ('NUMBER', Number.regex),
    ('BOOL', r'true|false'),
    ('NULL', r'null'),
])


def to_string(x):
    return x[1:-1]  # cheat!


parser = ox.make_parser([
    # Simple values
    ('value : object', lambda x: x),
    ('value : array', lambda x: x),
    ('value : NUMBER', lambda x: float(x)),
    ('value : BOOL', lambda x: x == 'true'),
    ('value : NULL', lambda x: None),
    ('value : STRING', lambda x: to_string(x)),

    ('object : LBRACE RBRACE', lambda _, __: {}),
    ('object : LBRACE members RBRACE', lambda _, pairs, __: dict(pairs)),

    ('members : pair', lambda x: x),
    ('members : pair COMMA members', lambda x, _, xs: [x] + xs),

    ('pair : STRING COLON value', lambda k, _, v: (k, v)),

    ('array : LBRACKET RBRACKET', lambda _, __: {}),
    ('array : LBRACKET elements RBRACKET', lambda _, xs, __: xs),

    ('elements : value', lambda x: [x]),
    ('elements : value COMMA elements', lambda x, _, xs: [x] + xs),
], tokens=[
    'LBRACE', 'RBRACE', 'LBRACKET', 'RBRACKET', 'COMMA', 'COLON', 'STRING',
    'NUMBER', 'BOOL', 'NULL'])


"""
Regex 101
"""


class Simple_match:
    """
    In in its simplest form, a regex simply compares itself with another string.

    For instance, the regex r'hello' simply matches the string 'hello'. This is 
    not very useful, but nevertheless, write a regex that matches the string
    'hello regex!'
    """
    regex = r''


class Group_of_characters:
    """
    What distinguishes regexes from simple string match is that some characters
    have special meanings and can be used to match more flexible patterns.

    By enclosing a group of characters inside brackets [abcde...] we are 
    declaring that we accept any of them in the given position. 

    Create a regular expression that matches either the string 'Hello!' or 
    the string 'hello!'.
    """
    regex = r'[hH]ello!'


class Ranges:
    """
    The [] grouping is very powerful. We can declare a group of characters or
    even a range of values. If characters appear sequentially (e.g., a, b, c, 
    ...) we can declare them as a ranges [a-cz-f].

    Create a grouping that matches any digit of an hexadecimal number.
    """
    regex = r'[0-9a-fA-F]'


class Be_careful_with_spaces:
    """
    Spaces inside a regex string are usually treated as literal space 
    characters. There are special flags that can disable this behavior by 
    simply ignoring spaces inside a regex string.

    Create a regex that matches a vowel followed by a single space and then a 
    digit. 
    """
    regex = r'[aeiou] [0-9]'


class Negated_matches:
    """
    Sometimes is useful to accept any character except the characters in a 
    group. We can negate a group of characters by starting the grouping with
    a caret such as in [^abc].

    Create a regex that matches anything but numbers.
    """
    regex = r'[^0-9]'


class Repetitions:
    """
    Now something very useful: we can declare repetitions of a pattern. 
    Regexes offer 3 basic ways to declare repetitions (we will look into more
    advanced patterns later).

        x* -- zero or more repetitions of x 
        x+ -- one or more repetitions of x
        x? -- one or zero repetitions of x (that is the same as saying that x 
              is optional)

    Of course we can exchange x by any other character or even by groups of 
    characters (such as in [abc]*)

    Create a regex that matches an integer number: it can have an optional plus
    or minus sign followed by a sequence of digits.
    """
    regex = r'[+-]?[0-9]+'
    max_length = len(regex)


class Advanced_repetitions:
    """
    We can control the number of repetitions more precisely using curly 
    brackets:

        x{n}   -- n repetitions
        x{n,m} -- between n and m repetitions
        x{n,}  -- at least n repetitions
        x{,m}  -- at most m repetitions

    Create a regular expression that matches an ISO date of form YYYY-MM-DD 
    """
    regex = r'[0-9]{4}-[0-9]{2}-[0-9]{2}'
    max_length = len(regex)


class Alternatives:
    """
    The [] notation allow us to choose many different characters that might
    appear on a given location. Sometimes we want to declare possible
    alternative substrings. That is what the pipe operator | is
    for: r'pattern1|pattern2' will match either pattern1 or pattern2,
    independently of the number of characters.

    Create a regex that matches either 'True' or 'False'
    """
    regex = r'True|False'
    max_length = len(regex)


class Grouping:
    """
    Sometimes we need to explicitly declare substrings in repetitions or as
    alternatives. Enclose the sub-pattern in parenthesis to define a group that
    is handled as a unity.

    Create a regex that matches either 'Hello world!' or 'Hello regex!'
    """
    regex = r'Hello (world|regex)!'
    max_length = len(regex)


class Grouping_hard:
    """
    Now modify your regex to match strings of the form 'Hello <full name>!' in 
    which <full name> is composed by one or more parts composed by a name that
    starts with an uppercase letter followed by one or more lowercase letters.
    """
    regex = r'Hello ([A-Z][a-z]+( [A-Z][a-z]+)*)!'


class Escaping:
    r"""
    What happens if you need to match an special character such as (, [ or +?

    Regexes allow us to escape them by preceeding the character with a
    backslash such as \+ or \\. Create a regex that matches strings of the
    form "<name> (<age>)"
    """
    regex = r'[A-Z][a-z]+ \([0-9]+\)'


class Escaping_inside_a_group:
    r"""
    All characters except ] and \ loose their special behavior inside bracket
    matches. The dash (-) and caret (^) can be escaped in order to loose their
    special meaning inside a bracket group.

    Create an expression that matches expressions of the form
    "<number><op><number>" such as "1+1"
    """
    regex = r'[0-9]+[-+*/^][0-9]+'





#
# Python basic
#


# Variables and basic operations

# Executing functions

# Basic string

# Print and input
"""
Interaction
===========
    
    author: Fábio Macêdo Mendes
    email: fabiomacedomendes@gmail.com

Python has two main functions for interacting with the user. The input(msg) 
asks the user for some input and return it as a string. The print(obj) function,
shows the content of the input on the screen.

Create a program that asks for the user name and save it on a variable called
*name*. Afterwards, it should print the message "Hello <name>!", replacing 
<name> with the given name.

Example:

    Name: <John Lennon>
    Hello John Lennon!

(User input is between <angle brackets>.)
"""

from maestro import fixtures

name = fixtures.name() 


def test_module(code):
    result = run_code(code, inputs=['John Lennon'])
    assert result == (..., 'John Lennon', 'Hello John Lennon!')


def test_synthetic_examples(code, name):
    result = run_code(code, inputs=[name])
    assert result == (..., name, 'Hello %s!' % name)



# Function definition

# If

# While loop

# For loop

# Basic string

# Basic list

# Basic dictionaries

# Basic sets

# Classes

# Function definition 2

# List comprehensions

# Dict comprehensions

# Set comprehensions

# Error handling

# Generators

# Iterators

# Decorators

# Destructuring assignment

# Else clause of loops

# f-strings

# Collections

# Closures

# Metaclasses
