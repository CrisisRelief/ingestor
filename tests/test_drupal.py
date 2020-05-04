import sys
from unittest.mock import patch, MagicMock

import pytest

from . import REPO_ROOT

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.drupal import Drupal
finally:
    sys.path = PATH


class TestDrupal:
    def setup_method(self):
        self.sessions = MagicMock()
        self.sessions.cookies = []

    def test_init_login_asserts_cookies(self):
        with \
                patch('requests.sessions.Session', self.sessions), \
                pytest.raises(UserWarning):
            Drupal(
                base_url='http://test.com/',
                username='username',
                password='password'
            )
