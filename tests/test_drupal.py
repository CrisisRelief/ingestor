import sys
from unittest.mock import patch, MagicMock
import responses
from urllib3.response import HTTPResponse
from os.path import join as path_join
import json
import pandas as pd

import pytest

from . import REPO_ROOT, TEST_DATA

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.drupal import Drupal
finally:
    sys.path = PATH


# class MockResponse:
#     class MockResponseRaw:
#         def __init__(self, text=None, headers=None):
#             self.text = text
#             self.headers =

#     def __init__(self, text=None, status_code=200, headers=None, history=None):
#         if headers is None:
#             headers = {}
#         if history is None:
#             history = []
#         self.text = text
#         self.status_code = status_code
#         self.headers = headers
#         self.history = history
#         self.is_redirect = False
#         self.raw = MockResponseRaw()

#     def json(self):
#         return json.loads(self.text)

#     @property
#     def content(self):
#         return self.text

# def mock_response(*args, **kwargs):
#     req = PreparedRequest()
#     raw_resp = HTTPResponse(*args, **kwargs)
#     resp = Response()
#     resp.status_code = getattr(raw_resp, 'status', None)
#     resp.headers = CaseInsensitiveDict(getattr(raw_resp, 'headers', {}))
#     resp.raw = raw_resp
#     resp.reason = resp.raw.reason
#     resp.cookies.extract_cookies(raw_resp, req)
#     return resp


# def mock_response_adapter(*args, **kwargs):
#     resp = mock_response(*args, **kwargs)
#     adapter = MagicMock()
#     adapter.send.return_value = resp
#     return adapter

class TestDrupal:
    def setup_method(self):
        self.sessions = MagicMock()
        self.sessions.cookies = []

    @responses.activate
    def test_init_login_asserts_cookies(self):
        # Given
        responses.add(responses.POST, 'http://test.com/user/login')

        # Then
        with pytest.raises(UserWarning):
            Drupal(
                base_url='http://test.com/',
                username='username',
                password='password'
            )

    @responses.activate
    def test_init_login_succeeds(self):
        # Given
        responses.add(
            'POST',
            'http://test.com/user/login',
            status=200,
            headers={
                'Set-Cookie': 'SSESSaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-ccccccc; expires=Thu, 28-May-2020 11:22:12 GMT; Max-Age=2000000; path=/; domain=.test.com; secure; HttpOnly'
            }
        )

        # When
        drupal = Drupal(
            base_url='http://test.com/',
            username='username',
            password='password'
        )

        # Then
        assert drupal.session.cookies

    @responses.activate
    def test_get_form_entry_meta_format(self):
        # Given
        responses.add(
            'POST',
            'http://test.com/user/login',
            status=200,
            headers={
                'Set-Cookie': 'SSESSaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-ccccccc; expires=Thu, 28-May-2020 11:22:12 GMT; Max-Age=2000000; path=/; domain=.test.com; secure; HttpOnly'
            }
        )
        dummy_meta_path = path_join(TEST_DATA, 'dummy-form-meta.json')
        with open(dummy_meta_path) as stream:
            responses.add(
                'POST',
                'http://test.com/jsonapi/webform_submission/resource_intake',
                status=200,
                json=json.load(stream)
            )
        drupal = Drupal(
            base_url='http://test.com/',
            username='username',
            password='password'
        )

        # When
        meta = drupal.get_form_entry_meta('resource_intake')

        # Then
        assert meta
        assert meta[0]['attributes']['drupal_internal__sid']
        assert meta[0]['attributes']['changed']

    @responses.activate
    def test_get_form_entries_df_format(self):
        # Given
        responses.add(
            'POST',
            'http://test.com/user/login',
            status=200,
            headers={
                'Set-Cookie': 'SSESSaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-ccccccc; expires=Thu, 28-May-2020 11:22:12 GMT; Max-Age=2000000; path=/; domain=.test.com; secure; HttpOnly'
            }
        )
        dummy_data_path = path_join(TEST_DATA, 'dummy-form-data-187.json')
        with open(dummy_data_path) as stream:
            responses.add(
                'POST',
                'http://test.com/webform_rest/resource_intake/submission/187',
                status=200,
                json=json.load(stream)
            )
        drupal = Drupal(
            base_url='http://test.com/',
            username='username',
            password='password'
        )

        # When
        entries = drupal.get_form_entries_df('resource_intake', [187])

        # Then
        assert not entries.empty
        assert isinstance(entries, pd.DataFrame)
