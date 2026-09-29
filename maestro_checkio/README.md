`maestro.checkio`
=================

A simple library and command line tool to read data from the checkio API.


## Installation

Just `pip install maestro.checkio` or use `flit install -s` from the project root if you want to help developing it from this repository.


## CLI Usage

Type `maestro-checkio --help` on the terminal for help. It has several options to fetch the grading data and returning a nicely formated table.

## Library

In most cases, `import maestro.checkio` and use one of the 3 main functions: `groups()`, `questions()` and `questions_dataframe()`.