from pathlib import Path

import pytest

from acras.config import Config


@pytest.fixture
def config(tmp_path: Path) -> Config:
    return Config(
        api_key="synthetic-test-key", base_url="https://example.invalid/api",
        database_url="postgresql://test.invalid/test", data_dir=tmp_path,
    )
