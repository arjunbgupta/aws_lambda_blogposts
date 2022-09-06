# -*- coding: utf-8 -*-
"""
    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""
import pytest


class LambdaContext:
    def __init__(self, function_name):
        self.function_name = function_name


@pytest.fixture(scope="session")
def lambda_context(request):
    return LambdaContext("test_lambda_context")
