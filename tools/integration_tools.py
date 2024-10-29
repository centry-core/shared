from typing import Optional

from pydantic import (
    BaseModel,
    root_validator,
)

from . import rpc_tools


def _find_first_configuration_by_partial_settings(
        user_id: int,
        project_id: int,
        configuration_personal: bool,
        integration_name: str,
        partial_settings: dict
):
    rpc_call = rpc_tools.RpcMixin().rpc.call
    personal_project_id = rpc_call.projects_get_personal_project_id(user_id)
    is_shared_project = rpc_call.admin_check_user_in_project(project_id, user_id)

    if not is_shared_project:
        raise RuntimeError(f"{user_id=} not in {project_id=}")

    if configuration_personal:
        integrations = rpc_call.integrations_get_all_integrations_by_name(
            personal_project_id,
            integration_name
        )
    else:
        integrations = rpc_call.integrations_get_all_integrations_by_name(
            project_id,
            integration_name
        )

    for i in integrations:
        for k, v in partial_settings.items():
            if i.settings.get(k) != v:
                break
        else:
            return i


def _find_config(
        project_id: int,
        configuration_personal: bool,
        integration_name: str,
        partial_settings: dict,
        configurations: list,
):
    if configuration_personal:
        integrations = [i for i in configurations if i.get('name') == integration_name]
    else:
        integrations = rpc_tools.RpcMixin().rpc.call.integrations_get_all_integrations_by_name(
            project_id,
            integration_name
        )
        integrations = [i.dict() for i in integrations]

    for i in integrations:
        for k, v in partial_settings.items():
            if i['settings'].get(k) != v:
                break
        else:
            return i


class ExternalIntegrationSupport(BaseModel):
    configuration_personal: bool = False
    configuration_title: str = None

    _integration_name = None
    _integration_fields = None

    def dict_expand_from_configurations(self, project_id: int, personal_configurations: list, **kwargs) -> dict:
        base_dict = super().dict(**kwargs)
        base_dict.pop('configuration_personal', None)
        base_dict.pop('configuration_title', None)

        if not isinstance(self._integration_fields, dict):
            integration_fields = {x: x for x in self._integration_fields}
        else:
            integration_fields = self._integration_fields

        partial_settings = {
            'title': self.configuration_title
        }
        integration = _find_config(
            project_id=project_id,
            configuration_personal=self.configuration_personal,
            integration_name=self._integration_name,
            partial_settings=partial_settings,
            configurations=personal_configurations
        )
        if integration is None:
            raise ValueError(f"Integration with title={self.configuration_title}' does not exist")

        for field, integration_field in integration_fields.items():
            base_dict[field] = integration['settings'].get(integration_field)

        return base_dict

    def dict_integration_expanded(self, user_id: int, project_id, **kwargs):
        """ Returns dict values with expanded external integration settings if exist"""

        base_dict = super().dict(**kwargs)
        base_dict.pop('configuration_personal', None)
        base_dict.pop('configuration_title', None)

        if not isinstance(self._integration_fields, dict):
            integration_fields = {x: x for x in self._integration_fields}
        else:
            integration_fields = self._integration_fields

        partial_settings = {
            'title': self.configuration_title
        }
        integration = _find_first_configuration_by_partial_settings(
            user_id,
            project_id,
            self.configuration_personal,
            self._integration_name,
            partial_settings
        )
        if integration is None:
            raise ValueError(f"Integration with title={self.configuration_title}' does not exist")

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
