from rest_framework import serializers

from tfc.utils.api_serializers import ApiTextErrorResponseSerializer


class ClickHouseHealthResponseSerializer(serializers.Serializer):
    status = serializers.ChoiceField(
        choices=("healthy", "degraded", "unhealthy", "disabled")
    )
    clickhouse_connected = serializers.BooleanField()
    clickhouse_v2_connected = serializers.BooleanField(
        help_text=(
            "Whether the CH25 HTTP endpoint used by the observe read path "
            "accepted the configured credentials."
        )
    )
    cdc_lag = serializers.DictField(child=serializers.FloatField())
    routing = serializers.DictField(child=serializers.JSONField())
    error = serializers.CharField(required=False)


class ClickHouseHealthErrorResponseSerializer(ApiTextErrorResponseSerializer):
    health_status = serializers.ChoiceField(
        choices=("healthy", "degraded", "unhealthy", "disabled"),
        required=False,
    )
    clickhouse_connected = serializers.BooleanField(required=False)
    cdc_lag = serializers.DictField(child=serializers.FloatField(), required=False)
    routing = serializers.DictField(child=serializers.JSONField(), required=False)
