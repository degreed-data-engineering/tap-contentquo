import json
from typing import Dict, Optional, Any, Iterable
from pathlib import Path
from singer_sdk import typing as th
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import SimpleAuthenticator
from singer_sdk.exceptions import FatalAPIError
import requests

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class TapContentQuoStream(RESTStream):
    """ContentQuo stream class."""

    _LOG_REQUEST_METRIC_URLS: bool = True
    token = None

    @property
    def url_base(self) -> str:
        """Base URL of source"""
        return self.config["api_base_url"]

    @property
    def http_headers(self) -> dict:
        """Return the HTTP headers needed."""
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Auth-Token": self.get_token(),
        }
        return headers

    def authenticate(self):
        """Authenticate and retrieve a new token."""
        auth_url = f"{self.url_base}/auth/authenticate"
        auth_payload = {
            "key": self.config.get("key"),
            "secret": self.config.get("secret"),
        }

        response = requests.post(auth_url, json=auth_payload)
        if response.status_code == 200:
            self.token = response.json().get("token")
        else:
            raise Exception(
                f"Failed to authenticate. Status code: {response.status_code}. Response: {response.text}"
            )

    def get_token(self) -> str:
        """Get the current token, authenticate if necessary."""
        if not self.token:
            self.authenticate()
        return self.token

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records for the stream, ensuring valid authentication and skipping records with non-200 responses."""
        self.get_token()  # Ensure valid token before making requests

        if context is None:
            self.logger.error("Context is None, cannot format URL.")
            return  # Skip processing

        url = f"{self.url_base}{self.path.format(**context)}"
        response = self.requests_session.get(url, headers=self.http_headers)

        if response.status_code != 200:
            self.logger.warn(f"Skipping record due to non-200 response: {response.status_code}")
            return  # Skip this record

        response_json = response.json()

        if response_json.get("issues") is not None:
            for record in response_json["issues"]:
                yield self.post_process(record, context)

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
        th.Property("eid", th.StringType),
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

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"eid": record["eid"]}


class EvaluationDetails(TapContentQuoStream):
    name = "evaluation_details"  # Stream name
    parent_stream_type = Evaluations
    path = "/evaluations/{eid}"  # API endpoint after base_url
    primary_keys = ["eid"]
    records_jsonpath = "$"  # Use requests response JSON to identify the JSON path
    replication_key = None

    schema = th.PropertiesList(
        th.Property("eid", th.StringType),
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


class EvaluationIssues(TapContentQuoStream):
    name = "evaluation_issues"
    parent_stream_type = Evaluations
    path = "/evaluations/{eid}/issues"
    primary_keys = ["id"]
    records_jsonpath = "$.issues[*]"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("categories", th.StringType),
        th.Property("fileName", th.StringType),
        th.Property("id", th.StringType),
        th.Property("segmentId", th.StringType),
        th.Property("severity", th.StringType),
        th.Property("status", th.StringType),
    ).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Request records for the stream, ensuring valid authentication and skipping records with non-200 responses."""
        self.get_token()  # Ensure valid token before making requests

        if context is None:
            self.logger.error("Context is None, cannot format URL.")
            return  # Skip processing

        url = f"{self.url_base}{self.path.format(**context)}"
        response = self.requests_session.get(url, headers=self.http_headers)

        if response.status_code != 200:
            self.logger.warn(f"Skipping record due to non-200 response: {response.status_code}")
            return  # Skip this record

        response_json = response.json()

        # Skip records where 'issues' is null
        if response_json.get("issues") is not None:
            for record in response_json["issues"]:
                yield self.post_process(record, context)
    
    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["eid"] = context["eid"]
        return row


class EvaluationMetrics(TapContentQuoStream):
    name = "evaluation_metrics"
    parent_stream_type = Evaluations
    path = "/evaluations/{eid}/metrics"
    primary_keys = ["evaluationId"]
    records_jsonpath = "$"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("evaluationId", th.StringType),
        th.Property("evaluationName", th.StringType),
        th.Property("metrics", th.StringType),
        th.Property("overallComment", th.StringType),
        th.Property("qualityProfile", th.StringType),
        th.Property("scope", th.StringType),
    ).to_dict()


class Users(TapContentQuoStream):
    name = "users"
    path = "/users"
    primary_keys = ["id"]
    records_jsonpath = "$[*]"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("username", th.StringType),
        th.Property("email", th.StringType),
        th.Property("groupId", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"id": record["id"]}


class UserDetails(TapContentQuoStream):
    name = "user_details"
    parent_stream_type = Users
    path = "/users/{id}"
    primary_keys = ["id"]
    records_jsonpath = "$"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("groupId", th.StringType),
        th.Property("username", th.StringType),
        th.Property("email", th.StringType),
        th.Property("firstname", th.StringType),
        th.Property("lastname", th.StringType),
        th.Property("fullname", th.StringType),
        th.Property("createdDate", th.StringType),
        th.Property("login", th.StringType),
        th.Property("role", th.StringType),
        th.Property("status", th.StringType),
        th.Property("languages", th.StringType),
    ).to_dict()
