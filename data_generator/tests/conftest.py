import pytest
from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates

@pytest.fixture(scope="session")
def original_df():
    return generate_billing_events()

@pytest.fixture(scope="session")
def updated_df(original_df):
    return generate_updates(original_df)

@pytest.fixture(scope="session")
def df(original_df):
    return original_df