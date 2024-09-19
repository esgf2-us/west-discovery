> [!CAUTION] Experimental

# search-stac-facade

Using Globus Search as a backend for stac-fastapi.

## Usage

### Scripts with pip-run

Scripts are written using `pip-run`.
You will need to install pip-run to use them:

    pipx install pip-run

Then you can run the scripts like this:

    # data scraper
    ./scrape.py

    # scrape + ingest
    ./ingest_data.py

### Globus Search Index Access

Ingest requires a write role on the Search index you are using.

Scripts are written to run against an Integration index.
