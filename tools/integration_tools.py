from typing import Optional

from pydantic import (
    BaseModel,
    root_validator,
)

from . import rpc_tools


class ExternalIntegrationSupport(BaseModel):
    integration_project_id: int
    integration_title: str

    _integration_name = None
    _integration_fields = None

    def dict_integration_expanded(self, user_id: int, **kwargs):
        """ Returns dict values with expanded external integration settings if exist"""

        base_dict = super().dict(**kwargs)
        base_dict.pop('integration_project_id', None)
        base_dict.pop('integration_title', None)

        if not isinstance(self._integration_fields, dict):
            integration_fields = {x: x for x in self._integration_fields}
        else:
            integration_fields = self._integration_fields

        partial_settings = {
            'title': self.integration_title
        }
        integration = rpc_tools.RpcMixin().rpc.call.integrations_find_first_integration_by_partial_settings(
            user_id,
            self.integration_project_id,
            self._integration_name,
            partial_settings
        )
        if integration is None:
            raise ValueError(f"Integration with title={self.integration_title}' does not exist")

        for field, integration_field in integration_fields.items():
            base_dict[field] = integration.settings.get(integration_field)

        return base_dict

    @root_validator(pre=True)
    def validate_inheritance(cls, values):
        assert all(
            f is not None for f in (cls._integration_name, cls._integration_fields)
        ), f"{cls} definition has missed integration fields"

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
