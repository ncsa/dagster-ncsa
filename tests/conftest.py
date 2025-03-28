import warnings


def pytest_configure(config):
    warnings.filterwarnings(
        "ignore",
        category=DeprecationWarning,
        message=".*Accessing this attribute on the instance is deprecated.*",
    )
