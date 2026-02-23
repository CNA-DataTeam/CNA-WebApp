from __future__ import annotations

from pathlib import Path
from typing import Any

# Organization and environment settings
ORG_DOMAIN = "clarkinc.biz"

# Potential local roots for SharePoint synced folders (OneDrive paths)
POTENTIAL_ROOTS = [
    Path.home() / "clarkinc.biz",
    Path.home() / "OneDrive - clarkinc.biz",
    Path.home() / "OneDrive",
]

# SharePoint document libraries where Task-Tracker might reside
DOCUMENT_LIBRARIES = [
    "Clark National Accounts - Documents",
    "Documents - Clark National Accounts",
]

# Relative path to the Task-Tracker directory within the document library
RELATIVE_APP_PATH = Path("Logistics and Supply Chain/Logistics Support/Task-Tracker")

# Preferred local Task-Tracker roots (new structure), checked before legacy discovery
TASK_TRACKER_ROOT_HINTS = [
    Path.home() / "clarkinc.biz" / "Clark National Accounts - Task-Tracker",
]

# Filenames for key resources
TASKS_XLSX_NAME = "TasksAndTargets.xlsx"
ACCOUNTS_XLSX_NAME = "CNA Personnel - Temporary.xlsx"

# Address validator output file
ADDRESS_VALIDATION_RESULTS_FILE = Path(
    r"\\therestaurantstore.com\920\Data\Reporting\FedEx Invoiced Data\Address Validation\Validator - Output\results.csv"
)

# Network directories for storing and retrieving data (UNC paths)
COMPLETED_TASKS_DIR = Path(
    r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Task-Tracker\CompletedTasks"
)
LIVE_ACTIVITY_DIR = Path(
    r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Task-Tracker\LiveTasks"
)
ARCHIVED_TASKS_DIR = Path(
    r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Task-Tracker\ArchivedTasks"
)
PERSONNEL_DIR = Path(r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Personnel")
LOGO_PATH = Path(r"\\therestaurantstore.com\920\Data\Reporting\Power BI Branding\CNA-Logo_Greenx4.png")

# Logging configuration
LOG_BASE_DIR = Path(r"\\therestaurantstore.com\920\Data\Logistics\Logistics App\Logs")
LOG_DIR = LOG_BASE_DIR  # Backward-compatible alias
LOG_USER_FILE_NAME = "CNA-App.log"  # LOG_BASE_DIR/<user>/<file>
LOG_FILE = LOG_BASE_DIR / LOG_USER_FILE_NAME  # Backward-compatible alias

# Application version
APP_VERSION = "2.0"

# Permission settings for analytics page (None or empty list means no restriction)
ALLOWED_ANALYTICS_USERS: list[str] | None = None


def _merge_packaging_config(defaults: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {k: v for k, v in defaults.items()}
    for section, default_section in defaults.items():
        raw_section = raw.get(section)
        if isinstance(default_section, dict):
            section_values = dict(default_section)
            if isinstance(raw_section, dict):
                section_values.update(raw_section)
            merged[section] = section_values
        else:
            merged[section] = raw_section if section in raw else default_section
    for key, value in raw.items():
        if key not in merged:
            merged[key] = value
    return merged


_PACKAGING_DEFAULTS: dict[str, Any] = {
    "ssas": {
        "connection": "",
        "database": "",
        "query": "",
        "access_token_env": "SSAS_ACCESS_TOKEN",
        "access_token_ttl_minutes": 55,
        "use_service_principal": False,
        "service_principal_tenant_env": "AZURE_TENANT_ID",
        "service_principal_client_id_env": "AZURE_CLIENT_ID",
        "service_principal_client_secret_env": "AZURE_CLIENT_SECRET",
        "service_principal_scope": "https://analysis.windows.net/powerbi/api/.default",
        "timeout_seconds": 60,
        "enable_mock": True,
    },
    "api": {
        "endpoint": "https://shippingcalculator-api.dev.clarkinc.biz/api/warehousepackager/estimatePackingRequirements",
        "timeout_seconds": 30,
        "retry_attempts": 4,
        "retry_backoff_seconds": 1.0,
        "enable_mock": True,
    },
    "logging": {
        "directory": str(LOG_BASE_DIR),
        "max_bytes": 1_048_576,
        "backup_count": 5,
    },
    "ui": {
        "default_warehouse": 920,
        "default_marginal_length": 0.0,
        "default_marginal_width": 0.0,
        "default_marginal_height": 0.0,
        "default_marginal_weight": 0.0,
    },
    "rules": [],
}

_PACKAGING_RAW: dict[str, Any] = {
    "ssas": {
        "connection": "powerbi://api.powerbi.com/v1.0/myorg/clark%20national%20accounts",
        "database": "Item Inventory Model",
        "query": "// DAX Query\nDEFINE\n\tVAR __DS0Core = \n\t\tSUMMARIZE(\n\t\t\tALLNOBLANKROW('Item Info'),\n\t\t\t'Item Info'[ItemNumber],\n\t\t\t'Item Info'[HeightInInches],\n\t\t\t'Item Info'[LengthInInches],\n\t\t\t'Item Info'[WidthInInches],\n\t\t\t'Item Info'[WeightInPounds],\n\t\t\t'Item Info'[IsRepackRequired],\n\t\t\t'Item Info'[IsRepositionable],\n\t\t\t'Item Info'[IsVerified],\n\t\t\t'Item Info'[Can Nest?],\n\t\t\t'Item Info'[AverageVolume],\n\t\t\t'Item Info'[BreakQuantity]\n\t\t)\n\nEVALUATE\n\t__DS0Core",
        "timeout_seconds": 60,
        "enable_mock": False,
    },
    "api": {
        "endpoint": "https://shippingcalculator-api.dev.clarkinc.biz/api/warehousepackager/estimatePackingRequirements",
        "timeout_seconds": 30,
        "retry_attempts": 4,
        "retry_backoff_seconds": 1.0,
        "enable_mock": True,
    },
    "logging": {
        "directory": str(LOG_BASE_DIR),
        "max_bytes": 1_048_576,
        "backup_count": 5,
    },
    "ui": {
        "default_warehouse": "920",
        "default_marginal_length": 0.0,
        "default_marginal_width": 0.0,
        "default_marginal_height": 0.0,
        "default_marginal_weight": 0.0,
    },
    "rules": [
        {
            "min_volume": 0,
            "max_volume": 1500,
            "package_count": 1,
        },
        {
            "min_volume": 1500,
            "max_volume": 3000,
            "package_count": 2,
        },
        {
            "min_volume": 3000,
            "max_volume": 6000,
            "package_count": 3,
        },
        {
            "min_volume": 6000,
            "max_volume": None,
            "package_count": 4,
        },
    ],
}

# Packaging page runtime configuration (single source of truth, replacing config.json).
PACKAGING_CONFIG: dict[str, Any] = _merge_packaging_config(_PACKAGING_DEFAULTS, _PACKAGING_RAW)