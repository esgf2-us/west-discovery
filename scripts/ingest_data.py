#!/usr/bin/env pip-run
# /// script
# dependencies = ["globus-sdk", "pystac-client"]
# ///

import itertools
import json
import os
import sys
import time

import globus_sdk
import globus_sdk.tokenstorage
import pystac_client

CEDA_ITEM_LIMIT = 1000
INGEST_BATCH_SIZE = 100

GLOBUS_ENVIRONMENT = "integration"
SEARCH_INDEX_ID = "0b717a04-73e4-47f8-a94d-1eddd8a07d49"

CEDA_STAC_URL = "https://api.stac.ceda.ac.uk/"
FIELD_MAPPING = {
    # currently we have issues with 'flattened' data
    # "assets": "flattened",
    "geometry": "geo_shape",
}


def main():
    stac_client = pystac_client.Client.open(CEDA_STAC_URL)
    globus_search_client = get_globus_search_client()

    tasks = []
    for item_batch in batched(
        capped_iter_items(stac_client, CEDA_ITEM_LIMIT), INGEST_BATCH_SIZE
    ):
        ingest_doc = {
            "ingest_type": "GMetaList",
            "ingest_data": {
                "gmeta": [format_item(item) for item in item_batch],
            },
            "field_mapping": FIELD_MAPPING,
        }
        try:
            res = globus_search_client.ingest(SEARCH_INDEX_ID, ingest_doc)
        except globus_sdk.SearchAPIError as err:
            print("Error submitting task:")
            print(json.dumps(err.raw_json, indent=2, separators=(",", ": ")))
            sys.exit(1)
        tasks.append(res["task_id"])
        print("submitted task:", res["task_id"])
    print("Finished submitting tasks.\n")
    print("Waiting for tasks to complete...")
    task_states = set()
    for task_id in tasks:
        res = _globus_search_task_wait(globus_search_client, task_id, indent="  - ")
        task_states.add(res["state"])
    if task_states == {"SUCCESS"}:
        print("\nDone.")
    else:
        print("\nSome tasks did not succeed.")
        sys.exit(1)


def batched(iterable, n):
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def iter_items(stac_client):
    search_results = stac_client.search(
        collections=["cmip6"],
        fields={"exclude": [], "include": ["properties"]},
    )
    for page in search_results.pages():
        for item in page.items:
            yield item


def capped_iter_items(stac_client, item_limit):
    for i, item in enumerate(iter_items(stac_client)):
        if i >= item_limit:
            break
        yield item


def format_item(item):
    content = item.to_dict()
    # embedded JSON data is used where we would like to use 'flattened' data
    content["assets"] = json.dumps(content["assets"], separators=(",", ":"))
    return {
        "subject": item.id,
        "content": content,
        "visible_to": ["public"],
    }


def get_globus_search_client() -> globus_sdk.SearchClient:
    auth_client = globus_sdk.NativeAppAuthClient(
        "d8a35547-d362-450d-9945-f0abdaaf68c6", environment=GLOBUS_ENVIRONMENT
    )
    file_adapter = globus_sdk.tokenstorage.SimpleJSONFileAdapter(
        os.path.expanduser("~/ceda-ingest-globus-search-tokens.json")
    )

    if not file_adapter.file_exists():
        response = do_login_flow(auth_client)
        file_adapter.store(response)
        tokens = response.by_resource_server[globus_sdk.SearchClient.resource_server]
    else:
        tokens = file_adapter.get_token_data(globus_sdk.SearchClient.resource_server)

    authorizer = globus_sdk.RefreshTokenAuthorizer(
        tokens["refresh_token"],
        auth_client,
        access_token=tokens["access_token"],
        expires_at=tokens["expires_at_seconds"],
        on_refresh=file_adapter.on_refresh,
    )
    return globus_sdk.SearchClient(
        authorizer=authorizer, environment=GLOBUS_ENVIRONMENT
    )


def do_login_flow(
    client: globus_sdk.NativeAppAuthClient,
) -> globus_sdk.OAuthTokenResponse:
    client.oauth2_start_flow(
        requested_scopes=globus_sdk.SearchClient.scopes.all, refresh_tokens=True
    )
    print(
        f"Please go to this URL and login:\n\n" f"{client.oauth2_get_authorize_url()}\n"
    )
    auth_code = input("Please enter the code here: ").strip()
    return client.oauth2_exchange_code_for_tokens(auth_code)


def _globus_search_task_wait(client, task_id, attempts=5, indent=""):
    res = client.get_task(task_id)
    wait_time = 0.5
    for _ in range(attempts):
        print(f"{indent}task({task_id}).state = {res['state']}", end="")
        if res["state"] not in ("SUCCESS", "FAILED"):
            print(f" sleep({wait_time})...")
            time.sleep(wait_time)
            wait_time *= 2
            # fetch the task again to update
            # MUST be done after sleep, or the final sleep of the loop won't achieve
            # anything
            res = client.get_task(task_id)
            continue
        print()
        break
    return res


if __name__ == "__main__":
    main()


# vim: ft=python
