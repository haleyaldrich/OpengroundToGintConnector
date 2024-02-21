import json
import pandas as pd
import requests

from mapper import OG_GINT_MAPPER


TOKEN = ''

def _get_project_id() -> str:
    # TODO
    return '5aa7bacc-8ea6-431b-bb6f-fa88e8e77d04'

def _remove_columns_with_all_none(df: pd.DataFrame) -> pd.DataFrame:
    """Removes all columns in a dataframe that are filled with NaNs."""
    to_drop = []
    for c in df.columns:        
        if sum(df[c].isnull()) == len(df):
            to_drop.append(c)

    if len(to_drop) > 0:
        df = df.drop(labels=to_drop, axis=1)

    return df

def _manipulate_df(response) -> pd.DataFrame:

    df = pd.json_normalize(
        response, record_path=['DataFields'], meta=['Id'])
    df = pd.pivot(df, index='Id', columns='Header', values='Value')
    df.columns = df.columns.map(lambda x: x.split(".")[-1]) # Remove GroupName
    df.columns.name = None
    df = df.reset_index()
    df.drop(columns=['Id'], inplace=True)

    df = _remove_columns_with_all_none(df)
    return df

def _get_projections_from_mapper(table: str) -> list[dict]:
    """Get Openground-formatted projections to specify headers in query."""

    try:
        headers = OG_GINT_MAPPER[table] # Get dictionary for table
    except KeyError:
        raise KeyError(f'Table {table} is not included in mapping dictionary')

    # Get headers for which there is a translation
    valid_headers = [k for k, v in headers.items() if v is not None]

    # Format headers to Openground projections
    projections = [{"Group": table, "Header": h} for h in valid_headers]

    return projections

def _get_payload(table: str) -> str:
    """Returns a JSON-serialized POST body for querying."""

    return json.dumps({
        "Projections": _get_projections_from_mapper(table),
        "Group": "LocationDetails",
        "Projects": [_get_project_id()]
        })

def query_table(token: str, table: str) -> pd.DataFrame:
    """Returns a dataframe with all the data in an Openground table."""

    headers = {
        'KeynetixCloud': 'U3VwZXJCYXRtYW5GYXN0',
        'Content-Type': 'application/json',
        'Expect': '100-continue',
        'instanceId': "0e26a63d-c309-49f8-8cad-863aa1bbcd52",
        'Authorization': f'Bearer {token}'
    }

    url = "https://api.us-east-1.openground.cloud/api/v1.0/data/query"
    payload = _get_payload(table)
    response = requests.request("POST", url, headers=headers, data=payload)

    if response.status_code == 200:
        return _manipulate_df(response.json())
    else:
        return response.text