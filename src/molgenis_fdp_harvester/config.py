# SPDX-FileCopyrightText: 2024-present Mark Janse <mark.janse@health-ri.nl>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
Configuration module for the Molgenis FDP Harvester.
"""
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class ConceptTableLink:
    """Schema for concept_table_link section."""
    # person: str
    dataset: str
    datasetseries: str
    kind: str
    publisher: str
    provenancestatement: str

    def __post_init__(self):
        """Validate that all fields are strings."""
        for field_name in ['kind', 'publisher', 'dataset', 'datasetseries', 'provenancestatement']:
            field_value = getattr(self, field_name)
            if not isinstance(field_value, str):
                raise TypeError(
                    f"Configuration 'concept_table_link.{field_name}' must be a string, "
                    f"got {type(field_value).__name__}"
                )


@dataclass
class HarvesterConfig:
    """Schema for harvester_config section."""
    auto_create_datasetseries: bool = True
    uri_lookup_config: Dict[str, Dict[str, str]] | None = None
    pid_service_url: str = None


@dataclass
class HarvesterConfigSchema:
    """Schema for the complete harvester configuration."""
    concept_table_link: ConceptTableLink
    harvester_config: HarvesterConfig | None = None


def validate_config(config_data: Dict[str, Any]) -> None:
    """Validate that the configuration contains all required sections and fields.

    Args:
        config_data: The parsed configuration dictionary

    Raises:
        ValueError: If the configuration is missing required sections or fields
        TypeError: If field types don't match the schema
    """
    try:
        # Validate concept_table_link
        if 'concept_table_link' not in config_data:
            raise ValueError("Configuration must contain a 'concept_table_link' section")
        concept_table_link = ConceptTableLink(**config_data['concept_table_link'])

        # Validate harvester_config if present
        harvester_config = None
        if 'harvester_config' in config_data:
            harvester_config = HarvesterConfig(**config_data['harvester_config'])

        # Validate complete schema
        HarvesterConfigSchema(
            concept_table_link=concept_table_link,
            harvester_config=harvester_config
        )

    except TypeError as e:
        raise ValueError(f"Invalid configuration: {e}") from e


def load_config(config_path):
    """Load and parse the configuration file."""
    with open(config_path, "rb") as f:
        config_data = tomllib.load(f)
    validate_config(config_data)
    return config_data
