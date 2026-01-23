"""
OpenAPI Tools for Flask-RESTful APIs.

Provides automatic OpenAPI spec generation by decorating API methods.
Works alongside the existing @auth.decorators.check_api() decorator.

Usage in plugin API:
    from tools.openapi_tools import openapi, openapi_registry

    class API(APIBase):
        url_params = ['<int:project_id>/<int:config_id>']

        @openapi(
            summary="Get Configuration",
            description="Get configuration by ID",
            response_model=ConfigurationDetails,
        )
        @auth.decorators.check_api({...})
        def get(self, project_id: int, config_id: int, **kwargs):
            ...

Usage in plugin module.py:
    from tools.openapi_tools import openapi_registry

    def init(self):
        self.descriptor.init_all()
        openapi_registry.register_plugin(
            plugin_name="configurations",
            version="1.0.0",
            api_module=self,  # Will auto-scan API classes
        )
"""
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Type, Union
from pydantic import BaseModel

from pylon.core.tools import log


def pydantic_to_openapi_schema(model: Type[BaseModel]) -> Dict[str, Any]:
    """Convert Pydantic model to OpenAPI schema."""
    try:
        schema = model.model_json_schema()
    except AttributeError:
        schema = model.schema()

    if "$defs" in schema:
        defs = schema.pop("$defs")
        if "definitions" not in schema:
            schema["definitions"] = {}
        schema["definitions"].update(defs)

    return schema


class OpenAPIRegistry:
    """
    Global registry for OpenAPI specifications.

    Collects endpoint metadata from decorated methods and generates specs.
    """

    def __init__(self):
        self._plugins: Dict[str, Dict] = {}
        self._endpoints: Dict[str, List[Dict]] = {}
        self._schemas: Dict[str, Dict] = {}

    def register_plugin(
        self,
        plugin_name: str,
        version: str = "1.0.0",
        description: str = "",
        tags: Optional[List[Dict]] = None,
    ) -> None:
        """Register a plugin for OpenAPI documentation."""
        self._plugins[plugin_name] = {
            "version": version,
            "description": description,
            "tags": tags or [{"name": plugin_name, "description": description}],
        }
        if plugin_name not in self._endpoints:
            self._endpoints[plugin_name] = []
        log.info(f"OpenAPI: Registered plugin '{plugin_name}' v{version}")

    def register_endpoint(
        self,
        plugin_name: str,
        path: str,
        method: str,
        summary: str,
        description: str = "",
        tags: Optional[List[str]] = None,
        parameters: Optional[List[Dict]] = None,
        request_body: Optional[Type[BaseModel]] = None,
        response_model: Optional[Type[BaseModel]] = None,
        responses: Optional[Dict] = None,
        security: Optional[List[Dict]] = None,
        deprecated: bool = False,
    ) -> None:
        """Register an API endpoint."""
        if plugin_name not in self._endpoints:
            self._endpoints[plugin_name] = []

        # Convert Flask URL params to OpenAPI format
        openapi_path = path.replace("<int:", "{").replace("<string:", "{").replace(">", "}")

        self._endpoints[plugin_name].append({
            "path": openapi_path,
            "method": method.lower(),
            "summary": summary,
            "description": description,
            "tags": tags or [plugin_name],
            "parameters": parameters or [],
            "request_body": request_body,
            "response_model": response_model,
            "responses": responses,
            "security": security,
            "deprecated": deprecated,
        })

    def register_schema(self, name: str, model: Type[BaseModel]) -> None:
        """Register a Pydantic model as a reusable schema."""
        self._schemas[name] = pydantic_to_openapi_schema(model)

    def get_plugin_spec(self, plugin_name: str) -> Dict[str, Any]:
        """Generate OpenAPI spec for a single plugin."""
        if plugin_name not in self._plugins:
            return {}

        plugin_info = self._plugins[plugin_name]

        spec = {
            "openapi": "3.0.3",
            "info": {
                "title": f"{plugin_name.replace('_', ' ').title()} API",
                "version": plugin_info["version"],
                "description": plugin_info["description"],
            },
            "paths": {},
            "components": {
                "schemas": {},
                "securitySchemes": {
                    "bearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT"
                    },
                    "sessionAuth": {
                        "type": "apiKey",
                        "in": "cookie",
                        "name": "session"
                    }
                }
            },
            "security": [{"bearerAuth": []}, {"sessionAuth": []}],
            "tags": plugin_info["tags"],
        }

        self._build_paths(spec, plugin_name)
        return spec

    def get_combined_spec(self, plugins: Optional[List[str]] = None) -> Dict[str, Any]:
        """Generate combined OpenAPI spec for multiple plugins."""
        spec = {
            "openapi": "3.0.3",
            "info": {
                "title": "Elitea Platform API",
                "version": "1.0.0",
                "description": "Combined API specification",
            },
            "paths": {},
            "components": {
                "schemas": {},
                "securitySchemes": {
                    "bearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT"
                    },
                    "sessionAuth": {
                        "type": "apiKey",
                        "in": "cookie",
                        "name": "session"
                    }
                }
            },
            "security": [{"bearerAuth": []}, {"sessionAuth": []}],
            "tags": [],
        }

        target_plugins = plugins or list(self._plugins.keys())

        for plugin_name in target_plugins:
            if plugin_name in self._plugins:
                spec["tags"].extend(self._plugins[plugin_name]["tags"])
                self._build_paths(spec, plugin_name)

        return spec

    def _build_paths(self, spec: Dict, plugin_name: str) -> None:
        """Build paths section for a plugin."""
        for endpoint in self._endpoints.get(plugin_name, []):
            path = endpoint["path"]
            method = endpoint["method"]

            if path not in spec["paths"]:
                spec["paths"][path] = {}

            operation = {
                "summary": endpoint["summary"],
                "description": endpoint["description"],
                "tags": endpoint["tags"],
                "parameters": endpoint["parameters"],
                "responses": endpoint["responses"] or {
                    "200": {"description": "Success"},
                    "400": {"description": "Bad request"},
                    "401": {"description": "Unauthorized"},
                    "403": {"description": "Forbidden"},
                    "404": {"description": "Not found"},
                    "500": {"description": "Server error"},
                },
            }

            if endpoint["deprecated"]:
                operation["deprecated"] = True

            if endpoint["security"]:
                operation["security"] = endpoint["security"]

            # Request body
            if endpoint["request_body"] and method in ["post", "put", "patch"]:
                model = endpoint["request_body"]
                schema_name = model.__name__
                spec["components"]["schemas"][schema_name] = pydantic_to_openapi_schema(model)
                operation["requestBody"] = {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {"$ref": f"#/components/schemas/{schema_name}"}
                        }
                    }
                }

            # Response model
            if endpoint["response_model"]:
                model = endpoint["response_model"]
                schema_name = model.__name__
                spec["components"]["schemas"][schema_name] = pydantic_to_openapi_schema(model)
                operation["responses"]["200"] = {
                    "description": "Success",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": f"#/components/schemas/{schema_name}"}
                        }
                    }
                }

            spec["paths"][path][method] = operation

    def list_plugins(self) -> List[str]:
        """List all registered plugins."""
        return list(self._plugins.keys())


# Global registry instance
openapi_registry = OpenAPIRegistry()


def openapi(
    summary: str,
    description: str = "",
    tags: Optional[List[str]] = None,
    parameters: Optional[List[Dict]] = None,
    request_body: Optional[Type[BaseModel]] = None,
    response_model: Optional[Type[BaseModel]] = None,
    responses: Optional[Dict] = None,
    deprecated: bool = False,
):
    """
    Decorator to document API methods with OpenAPI metadata.

    Use alongside @auth.decorators.check_api() - this decorator should come FIRST.

    Example:
        @openapi(
            summary="Get Configuration",
            description="Retrieves a configuration by ID",
            response_model=ConfigurationDetails,
            parameters=[
                {"name": "project_id", "in": "path", "required": True, "schema": {"type": "integer"}}
            ]
        )
        @auth.decorators.check_api({...})
        def get(self, project_id: int, config_id: int, **kwargs):
            ...
    """
    def decorator(func: Callable) -> Callable:
        # Store metadata on the function
        func._openapi = {
            "summary": summary,
            "description": description,
            "tags": tags,
            "parameters": parameters or [],
            "request_body": request_body,
            "response_model": response_model,
            "responses": responses,
            "deprecated": deprecated,
        }

        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper._openapi = func._openapi
        return wrapper

    return decorator


def extract_path_params_from_url(url_params: List[str]) -> List[Dict]:
    """
    Extract OpenAPI parameters from Flask url_params.

    Converts ['<int:project_id>/<int:config_id>'] to OpenAPI parameters.
    """
    params = []
    if not url_params:
        return params

    for url_param in url_params:
        parts = url_param.split("/")
        for part in parts:
            if part.startswith("<") and part.endswith(">"):
                # Parse <type:name> or <name>
                inner = part[1:-1]
                if ":" in inner:
                    param_type, param_name = inner.split(":", 1)
                else:
                    param_type = "string"
                    param_name = inner

                schema_type = "integer" if param_type == "int" else "string"

                params.append({
                    "name": param_name,
                    "in": "path",
                    "required": True,
                    "schema": {"type": schema_type},
                    "description": param_name.replace("_", " ").title(),
                })

    return params


def register_api_class(
    api_class: type,
    plugin_name: str,
    base_path: str,
    registry: OpenAPIRegistry = None,
) -> None:
    """
    Register all decorated methods from an API class.

    Args:
        api_class: The API class with @openapi decorated methods
        plugin_name: Plugin name for grouping
        base_path: Base URL path (e.g., "/api/v1/configurations")
        registry: OpenAPI registry (defaults to global)
    """
    if registry is None:
        registry = openapi_registry

    url_params = getattr(api_class, "url_params", [])
    path_params = extract_path_params_from_url(url_params)

    # Build full path
    if url_params:
        # Use first url_param pattern
        url_suffix = url_params[0]
        full_path = f"{base_path}/{url_suffix}" if url_suffix else base_path
    else:
        full_path = base_path

    # Convert to OpenAPI format
    full_path = full_path.replace("<int:", "{").replace("<string:", "{").replace(">", "}")

    for method_name in ["get", "post", "put", "delete", "patch"]:
        method = getattr(api_class, method_name, None)
        if method is None:
            continue

        # Check for _openapi metadata
        openapi_meta = getattr(method, "_openapi", None)
        if openapi_meta is None:
            continue

        # Merge path params with explicit params
        all_params = list(path_params)
        for param in openapi_meta.get("parameters", []):
            # Don't duplicate path params
            if not any(p["name"] == param["name"] for p in all_params):
                all_params.append(param)

        registry.register_endpoint(
            plugin_name=plugin_name,
            path=full_path,
            method=method_name,
            summary=openapi_meta["summary"],
            description=openapi_meta.get("description", ""),
            tags=openapi_meta.get("tags") or [plugin_name],
            parameters=all_params,
            request_body=openapi_meta.get("request_body"),
            response_model=openapi_meta.get("response_model"),
            responses=openapi_meta.get("responses"),
            deprecated=openapi_meta.get("deprecated", False),
        )


def scan_and_register_apis(
    plugin_name: str,
    api_module,
    base_api_path: str = "/api/v1",
    registry: OpenAPIRegistry = None,
) -> None:
    """
    Scan a plugin's API module and register all decorated endpoints.

    Args:
        plugin_name: Plugin name
        api_module: The plugin's api module or list of API classes
        base_api_path: Base API path prefix
        registry: OpenAPI registry
    """
    if registry is None:
        registry = openapi_registry

    # If it's a list of (path, class) tuples
    if isinstance(api_module, list):
        for path, api_class in api_module:
            register_api_class(api_class, plugin_name, path, registry)
    # If it's a module, try to find API classes
    elif hasattr(api_module, "__dict__"):
        for name, obj in api_module.__dict__.items():
            if isinstance(obj, type) and hasattr(obj, "url_params"):
                # Derive path from module name
                path = f"{base_api_path}/{plugin_name}/{name.lower()}"
                register_api_class(obj, plugin_name, path, registry)


def register_api_folder(
    api_package,
    plugin_name: str,
    base_path: str,
    registry: OpenAPIRegistry = None,
) -> int:
    """
    Automatically discover and register all API classes from a package/folder.

    Discovers all Python modules in the package that have an API class with url_params.
    Path is automatically derived: base_path/module_name (e.g., /api/v2/configurations/models)

    Args:
        api_package: The API package (e.g., from .api import v2 as api_v2)
        plugin_name: Plugin name for grouping
        base_path: Base URL path (e.g., "/api/v2/configurations")
        registry: OpenAPI registry (defaults to global)

    Returns:
        Number of API classes registered

    Example:
        from .api import v2 as api_v2

        register_api_folder(
            api_package=api_v2,
            plugin_name="configurations",
            base_path="/api/v2/configurations",
        )
        # Will register:
        #   configurations.py → /api/v2/configurations/configurations
        #   models.py → /api/v2/configurations/models
        #   types.py → /api/v2/configurations/types
    """
    import importlib
    import pkgutil

    if registry is None:
        registry = openapi_registry

    registered_count = 0

    # Get package path for discovery
    if hasattr(api_package, "__path__"):
        # It's a package, iterate through submodules
        for importer, module_name, is_pkg in pkgutil.iter_modules(api_package.__path__):
            if module_name.startswith("_"):
                continue

            try:
                # Import the submodule
                module = importlib.import_module(f"{api_package.__name__}.{module_name}")

                # Find API class in module
                api_class = getattr(module, "API", None)
                if api_class is None:
                    continue

                # Check if it has url_params (confirms it's a valid API class)
                if not hasattr(api_class, "url_params"):
                    continue

                # Path is base_path/module_name
                path = f"{base_path}/{module_name}"

                register_api_class(api_class, plugin_name, path, registry)
                registered_count += 1
                log.debug(f"OpenAPI: Registered {module_name}.API at {path}")

            except Exception as e:
                log.warning(f"OpenAPI: Failed to register {module_name}: {e}")

    else:
        # It's a module, look for API classes directly
        for name in dir(api_package):
            obj = getattr(api_package, name)
            if isinstance(obj, type) and hasattr(obj, "url_params") and name == "API":
                register_api_class(obj, plugin_name, base_path, registry)
                registered_count += 1

    log.info(f"OpenAPI: Registered {registered_count} endpoints from {api_package.__name__}")
    return registered_count
