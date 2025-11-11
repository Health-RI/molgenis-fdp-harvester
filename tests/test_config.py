# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from molgenis_fdp_harvester.config import validate_config, ConceptTableLink


def test_validate_config_valid():
    """Test validation with a valid configuration."""
    config_data = {
        'concept_table_link': {
            'person': 'persons',
            'dataset': 'datasets',
            'datasetseries': 'datasetseries'
        }
    }

    # Should not raise any exception
    validate_config(config_data)


def test_validate_config_missing_section():
    """Test validation fails when concept_table_link section is missing."""
    config_data = {}

    with pytest.raises(ValueError, match="Configuration must contain a 'concept_table_link' section"):
        validate_config(config_data)


def test_validate_config_missing_person():
    """Test validation fails when 'person' field is missing."""
    config_data = {
        'concept_table_link': {
            'dataset': 'datasets',
            'datasetseries': 'datasetseries'
        }
    }

    with pytest.raises(ValueError, match="Invalid configuration:"):
        validate_config(config_data)


def test_validate_config_missing_dataset():
    """Test validation fails when 'dataset' field is missing."""
    config_data = {
        'concept_table_link': {
            'person': 'persons',
            'datasetseries': 'datasetseries'
        }
    }

    with pytest.raises(ValueError, match="Invalid configuration:"):
        validate_config(config_data)


def test_validate_config_missing_datasetseries():
    """Test validation fails when 'datasetseries' field is missing."""
    config_data = {
        'concept_table_link': {
            'person': 'persons',
            'dataset': 'datasets'
        }
    }

    with pytest.raises(ValueError, match="Invalid configuration:"):
        validate_config(config_data)


def test_validate_config_invalid_type_person():
    """Test validation fails when 'person' is not a string."""
    config_data = {
        'concept_table_link': {
            'person': 123,  # Should be string
            'dataset': 'datasets',
            'datasetseries': 'datasetseries'
        }
    }

    with pytest.raises(ValueError, match="Configuration 'concept_table_link.person' must be a string, got int"):
        validate_config(config_data)


def test_validate_config_invalid_type_dataset():
    """Test validation fails when 'dataset' is not a string."""
    config_data = {
        'concept_table_link': {
            'person': 'persons',
            'dataset': ['datasets'],  # Should be string, not list
            'datasetseries': 'datasetseries'
        }
    }

    with pytest.raises(ValueError, match="Configuration 'concept_table_link.dataset' must be a string, got list"):
        validate_config(config_data)


def test_validate_config_invalid_type_datasetseries():
    """Test validation fails when 'datasetseries' is not a string."""
    config_data = {
        'concept_table_link': {
            'person': 'persons',
            'dataset': 'datasets',
            'datasetseries': None  # Should be string
        }
    }

    with pytest.raises(ValueError, match="Configuration 'concept_table_link.datasetseries' must be a string, got NoneType"):
        validate_config(config_data)


def test_concept_table_link_dataclass():
    """Test that ConceptTableLink dataclass can be instantiated correctly."""
    concept_table_link = ConceptTableLink(
        person='persons',
        dataset='datasets',
        datasetseries='datasetseries'
    )

    assert concept_table_link.person == 'persons'
    assert concept_table_link.dataset == 'datasets'
    assert concept_table_link.datasetseries == 'datasetseries'


def test_concept_table_link_from_dict():
    """Test that ConceptTableLink can be created from a dictionary."""
    config_dict = {
        'person': 'persons',
        'dataset': 'datasets',
        'datasetseries': 'datasetseries'
    }

    concept_table_link = ConceptTableLink(**config_dict)

    assert concept_table_link.person == 'persons'
    assert concept_table_link.dataset == 'datasets'
    assert concept_table_link.datasetseries == 'datasetseries'
