"""
This definition is a fork of the one from the Mongo backend for
stac-fastapi, modified to work on Globus Search.
"""

import typing as t

import attrs
import globus_sdk
from stac_fastapi.core import serializers
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException
from starlette.requests import Request

from .config import settings
from .convert import search_doc_to_stac_item
from .utility import get_project, list_projects

_client = settings.search_client

def cql_like_to_globus_like(pattern: str) -> str:
    out = []
    escaped = False

    for char in pattern:
        if escaped:
            out.append(char)
            escaped = False
        elif char == "\\":
            escaped = True
        elif char == "%":
            out.append("*")
        elif char == "_":
            out.append("?")
        else:
            out.append(char)

    if escaped:
        raise ValueError("CQL LIKE pattern cannot end with an escape character")

    return "".join(out)


def _extract_collection_ids(search: globus_sdk.SearchQuery) -> list[str] | None:
    collection_ids = []

    for filter_ in search.get("filters", ()):
        if (
            filter_.get("type") == "match_any"
            and filter_.get("field_name") == "collection"
        ):
            collection_ids.extend(filter_.get("values", ()))

    return collection_ids or None


def cql_to_filter(
    cql_query: dict[str, t.Any], collection_ids: list[str] | None = None
) -> dict[str, t.Any]:
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
            inner = cql_to_filter(cql_query["args"][0], collection_ids=collection_ids)
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
                "filters": [
                    cql_to_filter(inner, collection_ids=collection_ids)
                    for inner in cql_query["args"]
                ],
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
                    "field_name":  cql_query["args"][0]["property"],
                    "values": [cql_query["args"][1]],
                },
            }
        case "<" | ">":
            # we only have '<=' and '>=' in Search today
            raise NotImplementedError("'>' and '<' filters are not supported yet")
        case "isNull":
            
            # isNull => not(exists)
            return {
                "type": "not",
                "filter": {
                    "type": "exists", 
                    "field_name": cql_query["args"][0]["property"]
                },
            }
        case "<=":
            
            value = cql_query["args"][1]
            return {
                "type": "range",
                "field_name": cql_query["args"][0]["property"],
                "values": [{"from": "*", "to": value}],
            }
        case ">=":
            
            value = cql_query["args"][1]
            return {
                "type": "range",
                "field_name": cql_query["args"][0]["property"],
                "values": [{"from": value, "to": "*"}],
            }
        # ADVANCED COMPARISON OPERATORS (???)
        case "like":
            assert len(cql_query["args"]) == 2
            
            value = cql_query["args"][1]

            if not isinstance(value, str):
                raise ValueError("'like' filter requires a string pattern")

            return {
                "type": "like",
                "field_name": cql_query["args"][0]["property"],
                "value": cql_like_to_globus_like(value),
            }
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
            
            return {
                "type": "geo_shape",
                "field_name": cql_query["args"][0]["property"],
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
        return await run_in_threadpool(get_project, collection_id)

    async def get_all_collections(
        self, token: str | None, limit: int, request: Request
    ) -> tuple[list[dict[str, t.Any]], str | None]:
        return await run_in_threadpool(list_projects)

    async def get_one_item(self, collection_id: str, item_id: str) -> dict:
        res = await run_in_threadpool(
            _client.get_subject, settings.search_index_id, item_id
        )
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
            search["filters"].append(
                cql_to_filter(filter_, collection_ids=_extract_collection_ids(search))
            )
        return search

    @staticmethod
    def apply_free_text_filter(
        search: globus_sdk.SearchScrollQuery,
        free_text_queries: list[str] | None,
    ) -> globus_sdk.SearchScrollQuery:
        """Translate a list of free-text terms into a Globus Search query string.

        Terms are OR-joined so a result matching any term is returned, consistent
        with the stac-fastapi free-text extension spec. For field-scoped search
        the Globus Lucene syntax can be used directly via FreeTextAdvancedExtension.

        Args:
            search: The Globus SearchScrollQuery to modify.
            free_text_queries: Terms from the `q` request parameter, or None.

        Returns:
            The modified search object, or the original if no queries provided.
        """
        if not free_text_queries:
            return search
        query_string = " OR ".join(free_text_queries)
        search.set_query(query_string)
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

        if len(filters) == 0 and not search.get("q"):
            search.set_query("*")

        search.set_limit(limit)

        if token:
            search.set_marker(token)
        try:
            response = await run_in_threadpool(
                _client.scroll, settings.search_index_id, search
            )
        except globus_sdk.SearchAPIError as e:
            if e.http_status == 400:
                raise HTTPException(status_code=400, detail=e.message)
            raise
        return (
            [search_doc_to_stac_item(doc) for doc in response["gmeta"]],
            response["total"],
            response["marker"],
        )
