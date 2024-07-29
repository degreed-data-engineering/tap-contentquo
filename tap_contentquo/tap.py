from pathlib import Path
from typing import List
import logging
import click
from singer_sdk import Tap, Stream
from singer_sdk import typing as th

from tap_contentquo.streams import (
    Evaluations,
    EvaluationDetails,
    # QualityProfiles,
    EvaluationIssues,
    EvaluationMetadata,
    EvaluationMetrics,
    Users,
    UserDetails,
)

PLUGIN_NAME = "tap-contentquo"

STREAM_TYPES = [
    Evaluations,
    EvaluationDetails,
    # QualityProfiles,
    EvaluationIssues,
    EvaluationMetadata,
    EvaluationMetrics,
    Users,
    UserDetails,
]


class TapContentQuo(Tap):
    name = "tap-contentquo"
    config_jsonschema = th.PropertiesList(
        th.Property("api_url", th.StringType, required=True, description="API URL"),
        th.Property("key", th.StringType, required=True, description="Key"),
        th.Property("secret", th.StringType, required=True, description="Secret"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams = [stream_class(tap=self) for stream_class in STREAM_TYPES]

        return streams


# CLI Execution:
cli = TapContentQuo.cli
