"""
Swagger UI Endpoint.

Serves interactive Swagger UI for OpenAPI specs.

Endpoints:
- GET /api/v2/shared/swagger - Swagger UI for all plugins
- GET /api/v2/shared/swagger/<plugin_name> - Swagger UI for single plugin
"""
from pathlib import Path

from flask import Response, render_template_string

from tools import this
from ...tools.api_tools import APIBase


class API(APIBase):
    """Swagger UI endpoint."""

    url_params = [
        '',
        '<string:plugin_name>',
    ]

    def get(self, plugin_name: str = None, **kwargs):
        """
        Serve Swagger UI.

        Args:
            plugin_name: Plugin name for single-plugin spec (optional)
        """
        if plugin_name:
            spec_url = f"/api/v2/shared/openapi/{plugin_name}"
            title = f"Swagger UI - {plugin_name}"
        else:
            spec_url = "/api/v2/shared/openapi"
            title = "Swagger UI - All Plugins"

        # Load template from file
        template_path = Path(this.descriptor.path) / "templates" / "swagger.html"
        template_content = template_path.read_text()

        html = render_template_string(
            template_content,
            title=title,
            spec_url=spec_url,
        )

        return Response(html, mimetype='text/html')
