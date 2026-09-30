import os

import pytest

os.environ.setdefault("SEARCH_INDEX_ID", "test-search-index")


@pytest.fixture
def item_schema():
    return {
        "definitions": {
            "item_fields": {
                "properties": {
                    "activity_id": {
                        "type": "array",
                        "items": {"enum": ["CMIP", "ScenarioMIP"]},
                    },
                    "frequency": {"enum": ["mon", "day"]},
                    "variant_label": {"pattern": r"^r\d+i\d+p\d+f\d+$"},
                    "member_id": {
                        "anyOf": [
                            {"pattern": r"^r\d+i\d+p\d+f\d+$"},
                            {"pattern": r"^r\d+i\d+p\d+$"},
                        ]
                    },
                    "source_collection": {"type": "null"},
                }
            }
        }
    }
