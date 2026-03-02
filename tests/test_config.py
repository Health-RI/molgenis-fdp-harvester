# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from molgenis_fdp_harvester.config import validate_config, ConceptTableLink, HarvesterConfig

@pytest.fixture
def valid_config_data():
    return {
        'concept_table_link': {
            'dataset': 'datasets',
            'datasetseries': 'datasetseries',
            'kind': 'kind',
            'publisher': 'publisher',
            'provenancestatement': 'provenancestatement'
        }
    }


def test_validate_config_valid(valid_config_data):
    """Test validation with a valid configuration."""
    # Should not raise any exception
    validate_config(valid_config_data)


def test_validate_config_missing_section():
    """Test validation fails when concept_table_link section is missing."""
    config_data = {}

    with pytest.raises(ValueError, match="Configuration must contain a 'concept_table_link' section"):
        validate_config(config_data)


@pytest.mark.parametrize("missing_concept", ["kind", "publisher", "dataset", "datasetseries", "provenancestatement"])
def test_validate_config_missing_concept(valid_config_data, missing_concept):
    """Test validation fails when a concept field is missing."""
    invalid_config_data = valid_config_data.copy()
    invalid_config_data['concept_table_link'] = valid_config_data['concept_table_link'].copy()
    del invalid_config_data['concept_table_link'][missing_concept]

    with pytest.raises(ValueError, match="Invalid configuration:"):
        validate_config(invalid_config_data)


def test_validate_config_with_pid_service_url(valid_config_data):
    """Test validation with pid_service_url and fdp_id_prefix in harvester_config."""
    config_data = dict(valid_config_data)
    config_data['harvester_config'] = {
        'pid_service_url': 'https://pid.example.com',
        'fdp_id_prefix': 'testorg'
    }
    validate_config(config_data)


def test_harvester_config_defaults():
    """Test HarvesterConfig defaults for new PID fields."""
    config = HarvesterConfig()
    assert config.pid_service_url is None
    assert config.fdp_id_prefix is None
