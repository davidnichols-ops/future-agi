from django.db.models import Q
from rest_framework import serializers

from tracer.models.project import Project
from tracer.models.trace_session import TraceSession
from tracer.serializers.filters import (
    BOUNDED_PAGE_NUMBER_HELP_TEXT,
    ObserveGraphDataRequestSerializer,
    SortParamListQueryParamField,
    StrictInputSerializer,
    bounded_filter_list_query_param_field,
    filter_list_query_param_field,
)


class TraceSessionSerializer(serializers.ModelSerializer):
    project = serializers.PrimaryKeyRelatedField(
        queryset=Project.objects.all(), many=False
    )

    class Meta:
        model = TraceSession
        fields = ["id", "project", "bookmarked", "name", "created_at"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        request = self.context.get("request")
        if not request:
            return

        organization = getattr(request, "organization", None) or getattr(
            request.user, "organization", None
        )
        if not organization:
            return

        scope = Q(organization=organization)
        workspace = getattr(request, "workspace", None)
        if workspace:
            if getattr(workspace, "is_default", False):
                scope &= (
                    Q(workspace=workspace)
                    | Q(
                        workspace__is_default=True,
                        workspace__organization=organization,
                    )
                    | Q(workspace__isnull=True)
                )
            else:
                scope &= Q(workspace=workspace)

        project_manager = getattr(Project, "no_workspace_objects", Project.objects)
        self.fields["project"].queryset = project_manager.filter(
            scope,
            deleted=False,
        )


class TraceSessionFilterValuesQuerySerializer(serializers.Serializer):
    project_id = serializers.UUIDField(required=True)
    column = serializers.ChoiceField(
        choices=["session_id", "user_id", "first_message", "last_message"]
    )
    search = serializers.CharField(
        required=False,
        allow_blank=True,
        default="",
        max_length=512,
    )
    page = serializers.IntegerField(required=False, default=0, min_value=0)
    page_size = serializers.IntegerField(
        required=False, default=50, min_value=1, max_value=500
    )

    def validate_search(self, value):
        from tracer.services.clickhouse.attribute_reads import (
            InvalidAttributeSearch,
            validate_attribute_search,
        )

        try:
            return validate_attribute_search(value)
        except InvalidAttributeSearch as exc:
            raise serializers.ValidationError(str(exc)) from exc


class TraceSessionListQuerySerializer(StrictInputSerializer):
    project_id = serializers.UUIDField(required=False)
    user_id = serializers.CharField(required=False, allow_blank=True)
    bookmarked = serializers.BooleanField(required=False, allow_null=True)
    filters = bounded_filter_list_query_param_field(required=False, default=list)
    sort_params = SortParamListQueryParamField(required=False, default=list)
    page_number = serializers.IntegerField(
        required=False,
        default=0,
        min_value=0,
        help_text=BOUNDED_PAGE_NUMBER_HELP_TEXT,
    )
    page_size = serializers.IntegerField(
        required=False, default=30, min_value=1, max_value=500
    )
    interval = serializers.CharField(required=False, allow_blank=True)


class TraceSessionExportQuerySerializer(TraceSessionListQuerySerializer):
    project_id = serializers.UUIDField()


class TraceSessionRetrieveQuerySerializer(StrictInputSerializer):
    user_id = serializers.CharField(required=False, allow_blank=True)
    filters = filter_list_query_param_field(required=False, default=list)
    sort_params = SortParamListQueryParamField(required=False, default=list)
    page_number = serializers.IntegerField(required=False, default=0, min_value=0)
    page_size = serializers.IntegerField(
        required=False, default=30, min_value=1, max_value=500
    )


class TraceSessionGraphDataRequestSerializer(ObserveGraphDataRequestSerializer):
    pass
