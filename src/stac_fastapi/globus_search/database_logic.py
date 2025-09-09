"""
This definition is a fork of the one from the Mongo backend for
stac-fastapi, modified to work on Globus Search.
"""

import json
import os
import typing as t

import attrs
import globus_sdk
from stac_fastapi.core import serializers

from .config import SEARCH_INDEX_ID, GlobusSearchSettings
from .convert import search_doc_to_stac_item

_client: globus_sdk.SearchClient = GlobusSearchSettings().create_client


def cql_translate_fieldname(fieldname: str) -> str:
    if fieldname in ("id", "collection", "geometry"):
        return fieldname
    return f"properties.{fieldname}"


def cql_to_filter(cql_query: dict[str, t.Any]) -> dict[str, t.Any]:
    """
    Convert a CQL2 filter to a Globus Search filter.

    - raises NotImplementedError on any not-yet-implemented filter types
    - raises ValueError on filter types which are known not to be supportable
    - raises NotImplementedError on unrecognized filter types

    There are numerous draft standards without a clear final standard to follow.
    Our best reference right now for CQL2 definition is this OGC draft spec:
      https://docs.ogc.org/DRAFTS/21-065.html#temporal-functions
    """
    if "op" not in cql_query:
        return {}
    cql_op = cql_query["op"]

    # each group of matches is marked with one of the following qualifiers:
    # done: all done
    # partial: partly done, but some outstanding issues
    # todo: we can or should add this; may require research
    # rejected: some issue with supporting this makes it undesirable
    # ???: questions abound
    match cql_op:
        # BASIC CQL2 (partial)
        case "not":
            # convert the inner filter to a Search filter
            inner = cql_to_filter(cql_query["args"][0])
            # not(not(x)) == x
            if inner["type"] == "not":
                return inner["filter"]
            # wrap any inner filter in a not filter
            return {"type": "not", "filter": inner}
        case "and" | "or":
            # As of query#1.0.0, there is a direct mapping of cql2 to Search for
            # 'and' and 'or' ('op' --> 'type' and 'args' --> 'filter')
            return {
                "type": cql_op,
                "filters": [cql_to_filter(inner) for inner in cql_query["args"]],
            }
        case "=":
            # The CQL2 standards
            # (https://docs.ogc.org/is/21-065r2/21-065r2.html) are highly
            # detailed in their behavior and can express searches that are
            # perhaps far more rich than is possible in Search (or at least for
            # what Globus has implemented wrappers).

            # As I look at the filter list
            # (https://docs.globus.org/api/search/reference/post_query/#gfilter),
            # it strikes me that `match_all` is perhaps the closest analog. I
            # believe that CQL2 '=' works only for a single argument so it could
            # also be `match_any`.
            #
            # We have made the single arg assumption for several of these
            # without checks that it is true.
            assert len(cql_query["args"]) == 2
            return {
                "type": "match_any",
                "field_name": cql_query["args"][0]["property"],
                "values": [cql_query["args"][1]],
            }
        case "<>":
            # 'not match_all', see comments in '=' above
            assert len(cql_query["args"]) == 2
            return {
                "type": "not",
                "filter": {
                    "type": "match_any",
                    "field_name": cql_query["args"][0]["property"],
                    "values": [cql_query["args"][1]],
                },
            }
        case "<" | ">":
            # we only have '<=' and '>=' in Search today
            raise NotImplementedError("'>' and '<' filters are not supported yet")
        case "isNull":
            fieldname = cql_translate_fieldname(cql_query["args"][0]["property"])
            # isNull => not(exists)
            return {
                "type": "not",
                "filter": {"type": "exists", "field_name": fieldname},
            }
        case "<=":
            fieldname = cql_translate_fieldname(cql_query["args"][0]["property"])
            value = cql_query["args"][1]
            return {
                "type": "range",
                "field_name": fieldname,
                "values": [{"from": "*", "to": value}],
            }
        case ">=":
            fieldname = cql_translate_fieldname(cql_query["args"][0]["property"])
            value = cql_query["args"][1]
            return {
                "type": "range",
                "field_name": fieldname,
                "values": [{"from": value, "to": "*"}],
            }
        # ADVANCED COMPARISON OPERATORS (???)
        case "like":
            # requires regex queries? we may not want to support
            raise NotImplementedError("'like' filter is not supported yet")
        case "between":
            # range filter should work
            raise NotImplementedError("'between' filter is not supported yet")
        case "in":
            return {
                "type": "match_any",
                "field_name": cql_query["args"][0]["property"],
                "values": cql_query["args"][1],
            }
        # SPATIAL OPERATORS (partial)
        # note that this divides in the filter spec between "Basic Spatial Operators"
        # and "Spatial Operators"
        case "s_intersects" | "s_within":
            # geo shape query is in PR for Search at time of writing
            # it should look like this
            return {
                "type": "geo_shape",
                "relation": cql_op[2:],
                "shape": cql_query["args"][1],
            }
        case "s_contains" | "s_disjoint":
            # not included in the current implementation of geo query for Search
            # but easy to add on demand
            raise NotImplementedError(
                "'s_contains' and 's_disjoint' filters are not supported yet"
            )
        case "s_crosses" | "s_equals" | "s_overlaps" | "s_touches":
            # geo query types which ES does not support
            # it's conceivable that we could implement some of them by recombining
            # supported types
            # e.g., `(A intersects B) AND (B intersects A)` is approximately "equals"
            raise ValueError(f"The CQL filter type '{cql_op}' is not supported.")
        # ARRAY OPERATORS (rejected)
        case "a_equals" | "a_contains" | "a_contained_by" | "a_overlaps":
            # array operators likely require comparing values with a scripted query,
            # which is very costly to expose to users
            raise ValueError(f"The CQL filter type '{cql_op}' is not supported.")
        # ACCENT AND CASE INSENSITIVE COMPARISONS (???)
        case "casei" | "accenti":
            # case insensitive implies that the expectation is that all of the other
            # comparators are case sensitive, which is not the ES default and would need
            # special handling -- needs some clarification
            #
            # accent insensitive is the normal (analyzed text) behavior in ES
            # raises the same question -- do we need to make everything else accent
            # sensitive?
            raise ValueError(f"The CQL filter type '{cql_op}' is not supported.")
        # TEMPORAL FUNCTIONS (todo, ???)
        case "t_after" | "t_before" | "t_disjoint" | "t_equals" | "t_intersects":
            # these operators support comparison between intervals as well as
            # comparisons between "instants" (date-times) and intervals
            #
            # they may be possible to support today when the target is a date-time, but
            # we don't yet allow the 'date_range' type in Globus Search, so some of the
            # semantics would need exploration
            raise NotImplementedError(
                "temporal comparisons supporting intervals and "
                "instants are not supported yet"
            )
        case (
            "t_contains"
            | "t_during"
            | "t_finishedby"
            | "t_finishes"
            | "t_meets"
            | "t_metby"
            | "t_overlappedby"
            | "t_overlaps"
            | "t_startedby"
            | "t_starts"
        ):
            # these operators only support comparison between intervals
            # they may require that we have 'date_range' fields added to Search in order
            # to implement sensibly
            raise NotImplementedError(
                "temporal comparisons supporting intervals are not supported yet"
            )
        # ARITHMETIC EXPRESSIONS (rejected)
        case "+" | "-" | "*" | "/" | "%" | "div" | "^":
            # requires allowing modification of field values, probably demanding a
            # scripted query
            raise ValueError(f"The CQL filter type '{cql_op}' is not supported.")
        # not listed categories of filters:
        #
        # - property-property comparisons
        #   - property-property comparison requires use of scripted queries and would be
        #     very expensive
        #
        # - custom functions
        #   - custom functions is an arbitrary extension point -- unclear what this
        #     would even mean unless someone has functions they want us to implement
        case _:
            raise NotImplementedError(f"Unrecgonized operator: {cql_op}")

    return cql_op


@attrs.define
class DatabaseLogic:
    item_serializer: type[serializers.ItemSerializer] = attrs.field(
        default=serializers.ItemSerializer
    )
    collection_serializer: type[serializers.CollectionSerializer] = attrs.field(
        default=serializers.CollectionSerializer
    )

    async def find_collection(self, collection_id: str) -> dict:
        path = os.path.dirname(os.path.realpath(__file__))
        f = open(path + f"/schemas/{collection_id}.json")
        data = json.load(f)
        return data

    async def get_all_collections(
        self, token: str | None, limit: int, base_url: str
    ) -> tuple[list[dict[str, t.Any]], str | None]:
        collections = []
        path = os.path.dirname(os.path.realpath(__file__)) + "/schemas"
        for filename in os.listdir(path):
            f = open(path + f"/{filename}")
            collections.append(json.load(f))
        return collections, None

    async def get_one_item(self, collection_id: str, item_id: str) -> dict:
        res = _client.get_subject(SEARCH_INDEX_ID, item_id)
        return search_doc_to_stac_item(res.data)

    @staticmethod
    def make_search():
        return globus_sdk.SearchScrollQuery()

    @staticmethod
    def apply_ids_filter(search: globus_sdk.SearchQuery, item_ids: list[str]):
        search.add_filter("id", item_ids, type="match_any")
        return search

    @staticmethod
    def apply_collections_filter(
        search: globus_sdk.SearchQuery, collection_ids: list[str]
    ):
        search.add_filter("collection", collection_ids, type="match_any")
        return search

    @staticmethod
    def apply_intersects_filter(
        search: globus_sdk.SearchQuery, shape: dict[str, t.Any]
    ):
        # search.add_filter(...)
        return search

    @staticmethod
    def apply_bbox_filter(search: globus_sdk.SearchQuery, bbox: list[int]):
        west, south, east, north = bbox
        search["filters"] = search.get("filters", [])
        search["filters"].append(
            {
                "type": "geo_bounding_box",
                "field_name": "geometry",
                "top_left": {"lat": north, "lon": west},
                "bottom_right": {"lat": south, "lon": east},
            }
        )
        return search

    @staticmethod
    def apply_cql2_filter(
        search: globus_sdk.SearchQuery, filter_: dict[str, t.Any] | None
    ):
        if filter_:
            search["filters"] = search.get("filters", [])
            search["filters"].append(cql_to_filter(filter_))
        return search

    async def execute_search(
        self,
        search: globus_sdk.SearchQuery,
        limit: int,
        token: str | None,
        sort: dict[str, dict[str, str]] | None,
        collection_ids: list[str] | None,
        ignore_unavailable: bool = True,
    ) -> tuple[t.Iterable[dict[str, t.Any]], int | None, str | None]:
        filters = search.get("filters", ())

        if len(filters) == 0:
            search.set_query("*")

        search.set_limit(limit)

        if token:
            search.set_marker(token)

        try:
            response = _client.scroll(SEARCH_INDEX_ID, search)
            print(response["total"])
        except globus_sdk.SearchAPIError as e:
            print("SearchAPIError:")
            print(e.text)
            raise
        return (
            [search_doc_to_stac_item(doc) for doc in response["gmeta"]],
            response["total"],
            response["marker"],
        )
