"""
Figma Connector Example for Fivetran Connector SDK

This connector demonstrates how to sync Figma projects and files
using Figma's REST API. It requires a Figma personal access token
and a team ID. The connector fetches the projects for the team and
then lists the files within each project.
"""

import datetime
import requests as rq

from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op


def schema(configuration: dict):
    """Define the schema delivered by this connector."""
    return [
        {
            "table": "projects",
            "primary_key": ["id"],
            "columns": {
                "id": "STRING",
                "name": "STRING",
            },
        },
        {
            "table": "files",
            "primary_key": ["key"],
            "columns": {
                "key": "STRING",
                "name": "STRING",
                "last_modified": "UTC_DATETIME",
                "thumbnail_url": "STRING",
                "version": "STRING",
                "project_id": "STRING",
            },
        },
    ]


def update(configuration: dict, state: dict):
    """Fetch projects and files from Figma and yield operations."""
    token = configuration.get("figma_api_token")
    team_id = configuration.get("team_id")
    if not token or not team_id:
        raise ValueError("Both 'figma_api_token' and 'team_id' must be provided")

    headers = {"X-Figma-Token": token}
    base_url = "https://api.figma.com/v1"

    last_synced = state.get("last_synced")
    if last_synced:
        last_synced_dt = datetime.datetime.fromisoformat(last_synced.replace("Z", "+00:00"))
    else:
        last_synced_dt = None

    projects_url = f"{base_url}/teams/{team_id}/projects"
    log.info(f"Fetching projects from {projects_url}")
    response = rq.get(projects_url, headers=headers)
    response.raise_for_status()
    projects_data = response.json()

    for project in projects_data.get("projects", []):
        project_id = project.get("id")
        yield op.upsert(
            table="projects",
            data={"id": project_id, "name": project.get("name")},
        )

        files_url = f"{base_url}/projects/{project_id}/files"
        log.info(f"Fetching files from {files_url}")
        files_resp = rq.get(files_url, headers=headers)
        files_resp.raise_for_status()
        files_data = files_resp.json()

        for file in files_data.get("files", []):
            last_modified = file.get("last_modified")
            if last_synced_dt and last_modified:
                lm_dt = datetime.datetime.fromisoformat(last_modified.replace("Z", "+00:00"))
                if lm_dt <= last_synced_dt:
                    continue
            yield op.upsert(
                table="files",
                data={
                    "key": file.get("key"),
                    "name": file.get("name"),
                    "last_modified": last_modified,
                    "thumbnail_url": file.get("thumbnail_url"),
                    "version": file.get("version"),
                    "project_id": project_id,
                },
            )

    new_state = {
        "last_synced": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    }
    yield op.checkpoint(state=new_state)


connector = Connector(update=update, schema=schema)

if __name__ == "__main__":
    connector.debug()
