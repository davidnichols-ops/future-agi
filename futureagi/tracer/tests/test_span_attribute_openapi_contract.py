"""Runtime/OpenAPI parity for span-attribute discovery responses."""

import json
from pathlib import Path

import pytest

from tfc.utils.serializer_fields import JsonValueField
from tracer.serializers.span_attributes import (
    SpanAttributeDetailResponseSerializer,
    SpanAttributeKeySerializer,
    SpanAttributeTopValueSerializer,
    SpanAttributeValueSerializer,
)


def _swagger_definitions():
    path = (
        Path(__file__).resolve().parents[3]
        / "api_contracts"
        / "openapi"
        / "swagger.json"
    )
    with path.open() as schema_file:
        return json.load(schema_file)["definitions"]


TYPE_FIELDS = (
    (SpanAttributeKeySerializer, "SpanAttributeKey"),
    (SpanAttributeValueSerializer, "SpanAttributeValue"),
    (SpanAttributeDetailResponseSerializer, "SpanAttributeDetailResponse"),
)


@pytest.mark.parametrize(("serializer_cls", "definition_name"), TYPE_FIELDS)
def test_span_attribute_type_enum_matches_generated_openapi(
    serializer_cls, definition_name
):
    runtime_choices = list(serializer_cls().fields["type"].choices)
    openapi_choices = _swagger_definitions()[definition_name]["properties"]["type"][
        "enum"
    ]

    assert runtime_choices == ["string", "number", "boolean", "array"]
    assert openapi_choices == runtime_choices


@pytest.mark.parametrize(
    ("serializer_cls", "definition_name"),
    (
        (SpanAttributeValueSerializer, "SpanAttributeValue"),
        (SpanAttributeTopValueSerializer, "SpanAttributeTopValue"),
    ),
)
def test_span_attribute_json_values_match_generated_openapi(
    serializer_cls, definition_name
):
    field = serializer_cls().fields["value"]
    assert isinstance(field, JsonValueField)
    for value in ("Rejected", 7, 1.5, False, None, ["nested"], {"nested": True}):
        assert field.run_validation(value) == value

    value_schema = _swagger_definitions()[definition_name]["properties"]["value"]
    assert value_schema["x-json-value"] is True
    assert value_schema["x-nullable"] is True
