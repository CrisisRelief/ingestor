import requests
import pandas as pd


class Drupal:
    def __init__(self, base_url, username, password, login_form_id='user_login_form'):
        self.base_url = base_url
        self.login_form_id = login_form_id
        self.session = requests.Session()
        self.login(username, password)

    def login(self, username, password):
        resp = self.session.post(
            f'{self.base_url}user/login',
            data={
                'name': username,
                'pass': password,
                'form_id': self.login_form_id,
                'op': 'Log+in'
            }
        )
        if len(self.session.cookies) == 0:
            raise UserWarning(
                f"error logging in. response: {resp}\n{resp.headers}\n{resp.text}")

    def get_form_entry_meta(self, form_id):
        # TODO: test and implement this

        # Step 1: Get a list of webforms from f'{self.base_url}jsonapi/webform/webform'

        # Step 2: Validate that form_id exists in the forms from the previous step

        # Step 3: return the `data` attribute of the `json.loads` output for the form from f'{self.base_url}jsonapi/webform_submission/{form_id}'

        # e.g.:

        return [
            {
                "type": "webform_submission--resource_intake",
                "id": "e609cd63-4cc3-42ae-a943-e2dd62f2b87e",
                "links": {
                "self": {
                    "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e609cd63-4cc3-42ae-a943-e2dd62f2b87e"
                }
                },
                "attributes": {
                "serial": 5,
                "drupal_internal__sid": 86,
                "token": "_-j6SRIMzucYSPExJABJH_53VxvV1OHkMHuHPWGmgRo",
                "uri": "/addresource",
                "created": "2020-04-08T04:53:07+00:00",
                "completed": "2020-04-08T04:53:07+00:00",
                "changed": "2020-04-08T04:53:07+00:00",
                "in_draft": False,
                "current_page": None,
                "remote_addr": "114.198.124.100",
                "langcode": "en",
                "entity_type": "node",
                "entity_id": "82",
                "locked": False,
                "sticky": False,
                "notes": None,
                "metatag": None
                },
                "relationships": {
                "uid": {
                    "data": {
                    "type": "user--user",
                    "id": "dee5756d-48b4-41ac-957b-d1dbab3ea8f2"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e609cd63-4cc3-42ae-a943-e2dd62f2b87e/uid"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e609cd63-4cc3-42ae-a943-e2dd62f2b87e/relationships/uid"
                    }
                    }
                },
                "webform_id": {
                    "data": {
                    "type": "webform--webform",
                    "id": "05e5a681-41dc-46ab-847c-6bd8eea2b229"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e609cd63-4cc3-42ae-a943-e2dd62f2b87e/webform_id"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e609cd63-4cc3-42ae-a943-e2dd62f2b87e/relationships/webform_id"
                    }
                    }
                }
                }
            },
            {
                "type": "webform_submission--resource_intake",
                "id": "fa2373bd-a8a3-4aee-a0bf-cf3f878271f9",
                "links": {
                "self": {
                    "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/fa2373bd-a8a3-4aee-a0bf-cf3f878271f9"
                }
                },
                "attributes": {
                "serial": 6,
                "drupal_internal__sid": 108,
                "token": "EurgUUFgDUrzfvPO53VtUgC5L4HeP6deAUQJg1PtuG4",
                "uri": "/addresource",
                "created": "2020-04-09T06:05:16+00:00",
                "completed": "2020-04-09T06:05:16+00:00",
                "changed": "2020-04-09T06:05:16+00:00",
                "in_draft": False,
                "current_page": None,
                "remote_addr": "165.225.114.191",
                "langcode": "en",
                "entity_type": "node",
                "entity_id": "82",
                "locked": False,
                "sticky": False,
                "notes": None,
                "metatag": None
                },
                "relationships": {
                "uid": {
                    "data": {
                    "type": "user--user",
                    "id": "dee5756d-48b4-41ac-957b-d1dbab3ea8f2"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/fa2373bd-a8a3-4aee-a0bf-cf3f878271f9/uid"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/fa2373bd-a8a3-4aee-a0bf-cf3f878271f9/relationships/uid"
                    }
                    }
                },
                "webform_id": {
                    "data": {
                    "type": "webform--webform",
                    "id": "05e5a681-41dc-46ab-847c-6bd8eea2b229"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/fa2373bd-a8a3-4aee-a0bf-cf3f878271f9/webform_id"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/fa2373bd-a8a3-4aee-a0bf-cf3f878271f9/relationships/webform_id"
                    }
                    }
                }
                }
            },
            {
                "type": "webform_submission--resource_intake",
                "id": "e40f1719-bdee-40b3-8c04-b67a36d455b6",
                "links": {
                "self": {
                    "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e40f1719-bdee-40b3-8c04-b67a36d455b6"
                }
                },
                "attributes": {
                "serial": 7,
                "drupal_internal__sid": 109,
                "token": "ljA3U3vsZuwRHe0O9vk4LvRU1IqxWAmHJ7K9Bv1M_L4",
                "uri": "/form/resource-intake",
                "created": "2020-04-09T07:23:11+00:00",
                "completed": "2020-04-09T07:23:11+00:00",
                "changed": "2020-04-09T07:23:11+00:00",
                "in_draft": False,
                "current_page": None,
                "remote_addr": "103.96.5.31",
                "langcode": "en",
                "entity_type": None,
                "entity_id": None,
                "locked": False,
                "sticky": False,
                "notes": None,
                "metatag": None
                },
                "relationships": {
                "uid": {
                    "data": {
                    "type": "user--user",
                    "id": "65d3c2cc-0f22-4c1f-8250-2d90074b2895"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e40f1719-bdee-40b3-8c04-b67a36d455b6/uid"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e40f1719-bdee-40b3-8c04-b67a36d455b6/relationships/uid"
                    }
                    }
                },
                "webform_id": {
                    "data": {
                    "type": "webform--webform",
                    "id": "05e5a681-41dc-46ab-847c-6bd8eea2b229"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e40f1719-bdee-40b3-8c04-b67a36d455b6/webform_id"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/e40f1719-bdee-40b3-8c04-b67a36d455b6/relationships/webform_id"
                    }
                    }
                }
                }
            },
            {
                "type": "webform_submission--resource_intake",
                "id": "827f0c77-0fb5-4afb-9408-4c461281518a",
                "links": {
                "self": {
                    "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/827f0c77-0fb5-4afb-9408-4c461281518a"
                }
                },
                "attributes": {
                "serial": 8,
                "drupal_internal__sid": 187,
                "token": "0IoWfdSnJdtbQJyPfeu80aWXncTJzAo93nSdVQnAIKg",
                "uri": "/form/resource-intake",
                "created": "2020-04-28T02:48:07+00:00",
                "completed": "2020-04-28T02:48:07+00:00",
                "changed": "2020-04-28T02:48:07+00:00",
                "in_draft": False,
                "current_page": None,
                "remote_addr": "103.96.5.31",
                "langcode": "en",
                "entity_type": None,
                "entity_id": None,
                "locked": False,
                "sticky": False,
                "notes": None,
                "metatag": None
                },
                "relationships": {
                "uid": {
                    "data": {
                    "type": "user--user",
                    "id": "65d3c2cc-0f22-4c1f-8250-2d90074b2895"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/827f0c77-0fb5-4afb-9408-4c461281518a/uid"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/827f0c77-0fb5-4afb-9408-4c461281518a/relationships/uid"
                    }
                    }
                },
                "webform_id": {
                    "data": {
                    "type": "webform--webform",
                    "id": "05e5a681-41dc-46ab-847c-6bd8eea2b229"
                    },
                    "links": {
                    "related": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/827f0c77-0fb5-4afb-9408-4c461281518a/webform_id"
                    },
                    "self": {
                        "href": "https://test.crisis.app/jsonapi/webform_submission/resource_intake/827f0c77-0fb5-4afb-9408-4c461281518a/relationships/webform_id"
                    }
                    }
                }
                }
            }
        ]

    def get_form_entries_df(self, form_id, sids):
        # TODO: test and implement this

        # Step 1: Download the `data` from each sid using f'{self.base_url}webform_rest/{webform_id}/submission/{sid}'

        # Step 2: The output needs to be a dataframe, e.g. :

        rows = [
            {
                "contact_email": "foo@bar.com",
                "contact_name": "test contact name",
                "contact_phone": "test phone",
                "link": "test link",
                "public_contact": "0",
                "resource_category": [
                    "187"
                ],
                "resource_description": "test description",
                "resource_title": "testing title"
            }
        ]
        return pd.DataFrame(rows)
