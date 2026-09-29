import pytest

from pathlib import Path

TEST_DIR: Path = Path(__file__).parent
REPO_DIR: Path = TEST_DIR.parent
DATA_DIR: Path = TEST_DIR / "data"


@pytest.fixture
def data():
    def data(path) -> str:
        return (DATA_DIR / path).read_text()

    return data
