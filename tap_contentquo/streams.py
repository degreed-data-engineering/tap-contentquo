import base64
import json
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing
from functools import cached_property
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.exceptions import FatalAPIError
import requests


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TapContentQuoStream(RESTStream):
    """ContentQuo stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return self.config["api_url"]

    @property
    def http_headers(self) -> dict:
        """Return the HTTP headers needed."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        return headers

    @property
    def authenticator(self):
        auth_url = f"{self.url_base}/auth/authenticate"
        auth_payload = {
            "key": self.config.get("api_key"),
            "secret": self.config.get("api_secret"),
        }

        response = requests.post(auth_url, json=auth_payload)
        if response.status_code == 200:
            token = response.json().get("token")
            auth_headers = {"X-Auth-Token": token}
            return SimpleAuthenticator(stream=self, auth_headers=auth_headers)
        else:
            raise Exception(
                f"Failed to authenticate. Status code: {response.status_code}"
            )


class Evaluations(TapContentQuoStream):
    name = "evaluations"  # Stream name
    path = "/evaluations"  # API endpoint after base_url
    primary_keys = ["eid"]
    records_jsonpath = (
        "$.evaluations[*]"  # Use requests response JSON to identify the JSON path
    )
    replication_key = None

    schema = th.PropertiesList(
        th.Property("analyticalIssueCount", th.IntegerType),
        th.Property("eid", th.StringType),
        th.Property("name", th.StringType),
        th.Property("created", th.StringType),
        th.Property("currentStep", th.StringType),
        th.Property("editCount", th.IntegerType),
        th.Property("groupID", th.IntegerType),
        th.Property("groupName", th.StringType),
        th.Property("profileID", th.StringType),
        th.Property("projectID", th.IntegerType),
        th.Property(
            "qualityResult",
            th.ObjectType(
                th.Property(
                    "analytical",
                    th.ObjectType(
                        th.Property("gradeIndex", th.IntegerType),
                        th.Property("gradeName", th.StringType),
                        th.Property("score", th.NumberType),
                    ),
                ),
                th.Property(
                    "auto",
                    th.ObjectType(
                        th.Property("characTer", th.StringType),
                        th.Property("tausEditDensity", th.StringType),
                    ),
                ),
                th.Property("holistic", th.StringType),
            ),
        ),
        th.Property(
            "scope",
            th.ObjectType(
                th.Property("fileCount", th.IntegerType),
                th.Property("volume", th.IntegerType),
            ),
        ),
        th.Property("scoredAnalyticalIssueCount", th.IntegerType),
        th.Property("srcLocale", th.StringType),
        th.Property("started", th.StringType),
        th.Property("tgtLocale", th.StringType),
        th.Property("translatorID", th.IntegerType),
        th.Property("workflowID", th.IntegerType),
        th.Property("workflowName", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"eid": record["eid"]}


class EvaluationDetails(TapContentQuoStream):
    name = "evaluation_details"  # Stream name
    parent_stream_type = Evaluations
    path = "/evaluations/{eid}"  # API endpoint after base_url
    primary_keys = ["id"]
    records_jsonpath = "$"  # Use requests response JSON to identify the JSON path
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("analyticalIssueCount", th.IntegerType),
        th.Property(
            "assignees",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "assignments",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("role", th.StringType),
                            )
                        ),
                    ),
                    th.Property("fullname", th.StringType),
                    th.Property("userId", th.IntegerType),
                )
            ),
        ),
        th.Property("comments", th.StringType),
        th.Property("created", th.StringType),
        th.Property("currentStep", th.StringType),
        th.Property("editCount", th.IntegerType),
        th.Property("groupID", th.IntegerType),
        th.Property("groupName", th.StringType),
        th.Property("name", th.StringType),
        th.Property("profileID", th.StringType),
        th.Property("projectID", th.IntegerType),
        th.Property(
            "qualityResult",
            th.ObjectType(
                th.Property(
                    "analytical",
                    th.ObjectType(
                        th.Property("gradeIndex", th.IntegerType),
                        th.Property("gradeName", th.StringType),
                        th.Property("score", th.NumberType),
                    ),
                ),
                th.Property(
                    "auto",
                    th.ObjectType(
                        th.Property("characTer", th.StringType),
                        th.Property("tausEditDensity", th.StringType),
                    ),
                ),
                th.Property("holistic", th.StringType),
            ),
        ),
        th.Property(
            "scope",
            th.ObjectType(
                th.Property("fileCount", th.IntegerType),
                th.Property("volume", th.IntegerType),
            ),
        ),
        th.Property("scoredAnalyticalIssueCount", th.IntegerType),
        th.Property("srcLocale", th.StringType),
        th.Property("started", th.StringType),
        th.Property("tgtLocale", th.StringType),
        th.Property("translatorID", th.IntegerType),
        th.Property("workflowID", th.IntegerType),
        th.Property("workflowName", th.StringType),
    ).to_dict()

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["eid"] = context["eid"]
        return row

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records for the stream, handling 404 errors specifically."""
        try:
            yield from super().request_records(context)
        except FatalAPIError as e:
            if "404 Client Error" in str(e):
                self.logger.warn(
                    f"Evaluation ID {context.get('eid')} not found. Skipping."
                )
            else:
                raise
