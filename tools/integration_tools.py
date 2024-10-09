from typing import Optional

from pydantic import (
    BaseModel,
    root_validator,
)

from . import rpc_tools


class ExternalIntegrationSupport(BaseModel):
    integration_user_id: Optional[int] = None
    integration_project_id: Optional[int] = None
    integration_title: Optional[str] = None

    _integration_name = None
    _integration_fields = None

    def dict_auto_exclude(self, **kwargs):
        if self.integration_project_id is not None:
            excluded = self._integration_fields
            if kwargs.get('exclude') is None:
                kwargs['exclude'] = set(excluded)
            else:
                kwargs['exclude'].update(excluded)
        return super().dict(**kwargs)

    @root_validator(pre=True)
    def validate_by_available_integration(cls, values):
        assert all(
            f is not None for f in (cls._integration_name, cls._integration_fields)
        ), f"{cls} definition has missed integration fields"

        if not isinstance(cls._integration_fields, dict):
            integration_fields = {x:x for x in cls._integration_fields}
        else:
            integration_fields = cls._integration_fields

        title = values.get('integration_title')
        integration_project_id = values.get('integration_project_id')

        args = bool(title) + bool(integration_project_id)
        if args == 1:
            raise ValueError("both non-empty integration_project_id and integration_title are required or none of them")

        if title is None:
            return values

        project_id = values.setdefault('integration_user_id', _get_current_user_or_none())
        if not project_id:
            raise ValueError("Missing integration_user_id field")

        partial_settings = {
            'title': title
        }
        integration = rpc_tools.RpcMixin().rpc.call.integrations_find_first_integration_by_partial_settings(
            project_id,
            int(integration_project_id),
            cls._integration_name,
            partial_settings
        )
        if integration is None:
            raise ValueError(f"Integration with '{title=}' does not exist")

        for field, integration_field in integration_fields.items():
            values[field] = integration.settings.get(integration_field)

        return values


def generate_create_integration_settings_model_from(IntegrationModel, integration_name):
    class CreateIntegrationModel(IntegrationModel):
        project_id: Optional[int] = None

        class Config:
            fields = {
                "project_id": {"exclude": True},
            }

        @root_validator(allow_reuse=True)
        def validate_unique_title(cls, values):
            title = values.get('title')

            integrations = rpc_tools.RpcMixin().rpc.call.integrations_get_integrations_by_setting_value(
                values.get('project_id'),
                integration_name,
                "title",
                title
            )

            if len(integrations) > 0:
                raise ValueError(f"{integration_name} with {title=} already exists")

            return values

    CreateIntegrationModel._desc = integration_name
    return CreateIntegrationModel


def _get_current_user_or_none():
    from tools import auth
    try:
        user_id = auth.current_user().get('id')
    except Exception as ex:
        user_id = None

    return user_id
