import pytest

from eventsourcing_helpers.utils import get_callable_representation


class Test:
    def method(self):
        pass

    @classmethod
    def klass(cls):
        pass

    @staticmethod
    def static():
        pass


def myfunc():
    pass


@pytest.mark.parametrize('result, expected_result', [
    (get_callable_representation(Test), 'Test'),
    (get_callable_representation(Test()), ''),
    (get_callable_representation(Test().method), 'Test.method'),
    (get_callable_representation(Test.method), 'Test.method'),
    (get_callable_representation(Test().klass), 'Test.klass'),
    (get_callable_representation(Test.klass), 'Test.klass'),
    (get_callable_representation(Test().static), 'Test.static'),
    (get_callable_representation(Test.static), 'Test.static'),
    (get_callable_representation(myfunc), 'myfunc'),
    (get_callable_representation(None), ''),
    (get_callable_representation(1), ''),
])
def test_get_callable_representation(result, expected_result):
    assert expected_result == result
