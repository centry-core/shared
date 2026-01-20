"""
OpenAPI Specification Endpoint.

Serves OpenAPI specs for registered plugins.

Endpoints:
- GET /api/v1/shared/openapi - Combined spec for all plugins
- GET /api/v1/shared/openapi/<plugin_name> - Single plugin spec
- GET /api/v1/shared/openapi/plugins - List registered plugins
"""
from flask import request, Response

from ...tools.api_tools import APIBase
from ...tools.openapi_tools import openapi_registry


class API(APIBase):
    """OpenAPI specification endpoint."""

    url_params = [
        '',
        '<string:plugin_name>',
    ]

    def get(self, plugin_name: str = None, **kwargs):
        """
        Get OpenAPI specification.

        Args:
            plugin_name: Plugin name for single-plugin spec, or "plugins" to list

        Query Parameters:
            format: 'json' (default) or 'yaml'
            plugins: Comma-separated list of plugins (for combined spec)
        """
        output_format = request.args.get('format', 'json').lower()

        # List registered plugins
        if plugin_name == "plugins":
            return {"plugins": openapi_registry.list_plugins()}, 200

        # Single plugin spec
        if plugin_name:
            spec = openapi_registry.get_plugin_spec(plugin_name)
            if not spec:
                return {
                    "error": f"Plugin '{plugin_name}' not found",
                    "available": openapi_registry.list_plugins()
                }, 404
        else:
            # Combined spec
            plugins_filter = request.args.get('plugins')
            if plugins_filter:
                plugins = [p.strip() for p in plugins_filter.split(',')]
            else:
                plugins = None
            spec = openapi_registry.get_combined_spec(plugins)

        # Output format
        if output_format == 'yaml':
            try:
                import yaml
                yaml_content = yaml.dump(spec, default_flow_style=False, sort_keys=False, allow_unicode=True)
                return Response(
                    yaml_content,
                    mimetype='application/x-yaml',
                    headers={'Content-Disposition': f'inline; filename=openapi.yaml'}
                )
            except ImportError:
                return {"error": "YAML not available. Install PyYAML."}, 501

        return spec, 200
