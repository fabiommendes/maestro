GENERIC_EXCLUDE_PATTERNS = [
    ".git/",
    ".gitignore",
]

EDITOR_EXCLUDE_PATTERNS = [
    ".vscode/",
    ".idea/",
]

PYTHON_EXCLUDE_PATTERNS = [
    "**/__pycache__/",
    "**/*.pyc",
    "**/*.pyo",
    "**/*.pyd",
    "**/*.pyo",
    ".mypy_cache/",
    ".pytest_cache/",
    ".venv/",
    "venv/",
    "**/site-packages/",
]

UV_EXCLUDE_PATTERNS = [
    *GENERIC_EXCLUDE_PATTERNS,
    *EDITOR_EXCLUDE_PATTERNS,
    *PYTHON_EXCLUDE_PATTERNS,
]

UV_PROTECTED_PATTERNS = [
    "pyproject.toml",
    "uv.lock",
    "README.md",
    "tests/",
    "pytest.ini",
]
