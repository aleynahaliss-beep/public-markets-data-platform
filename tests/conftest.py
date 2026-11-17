import pytest

from pmdp.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("pmdp-tests", shuffle_partitions=2)
    yield session
    session.stop()
