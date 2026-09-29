import pytest


def test_right():
    assert 1 == 1


def test_wrong():
    assert 1 == 2


class TestClass:
    def test_method_right(self):
        assert 2 == 2

    def test_method_wrong(self):
        assert 2 == 3


@pytest.mark.parametrize("node", [1, 2, 3])
def test_parametric(node):
    assert node in (1, 2)


@pytest.mark.grade(3.0, feedback="This is a graded test.")
def test_graded():
    assert 1 + 1 == 2


@pytest.mark.grade(3.0, feedback="Is math wrong?")
def test_fail_with_feedback():
    assert 1 + 1 == 3


def test_error():
    raise ValueError("This is an error")
