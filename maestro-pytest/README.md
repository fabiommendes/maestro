# Maestro Pytest Plugin

A simple Pytest plugin that helps using Pytest as a grading tool.

## Installation

You can install the plugin via pip:

```bash
pip install maestro-pytest-plugin
```

## Usage

1. Configure pytest as you would normally. To enable the Maestro plugin, add the following to your `pytest.ini` file:

   ```ini
   [pytest]
   addopts = --maestro
   ```

2. Run your tests with pytest as usual:

   ```bash
   pytest
   ```
   The plugin will show a small summary at the end of the test run with
   assigning points to each test. 


## Configuration

You can configure the plugin by adding options to one of the following configuration 
files:

1) maestro-test.toml
2) pytest.ini
3) pyproject.toml

You can either run the cli tool `maestro-test init` to initialize a 
configuration file or add the options manually using the following schema:

```ini
[config]
id = "maestro-pytest"

[grades]
total = 100.0
passed = 1.0
failed = 0.0
error = 0.0
```

## Configuring tests

By default, each test is worth 1 point. You can customize the points for each test
by using the `@pytest.mark.grade` decorator:

```python
import pytest

@pytest.mark.grade(2, feedback="This message is shown if the test fails")
def test_example():
    assert True
```

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.