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
    person: str
    dataset: str
    datasetseries: str


@dataclass
class HarvesterConfigSchema:
    """Schema for the complete harvester configuration."""
    concept_table_link: ConceptTableLink


def validate_config(config_data: Dict[str, Any]) -> None:
    """Validate that the configuration contains all required sections and fields.

    Args:
        config_data: The parsed configuration dictionary

    Raises:
        ValueError: If the configuration is missing required sections or fields
        TypeError: If field types don't match the schema
    """
    # Check for concept_table_link section
    if 'concept_table_link' not in config_data:
        raise ValueError("Configuration must contain a 'concept_table_link' section")

    concept_table_link_dict = config_data['concept_table_link']

    # Try to construct the dataclass - this validates all required fields exist
    try:
        concept_table_link = ConceptTableLink(**concept_table_link_dict)
    except TypeError as e:
        # This catches missing fields or extra unexpected fields
        raise ValueError(f"Invalid 'concept_table_link' section: {e}") from e

    # Validate that all values are strings
    for field_name in ['person', 'dataset', 'datasetseries']:
        field_value = getattr(concept_table_link, field_name)
        if not isinstance(field_value, str):
            raise TypeError(
                f"Configuration 'concept_table_link.{field_name}' must be a string, "
                f"got {type(field_value).__name__}"
            )


def load_config(config_path):
    """Load and parse the configuration file."""
    with open(config_path, "rb") as f:
        config_data = tomllib.load(f)
    validate_config(config_data)
    return config_data
