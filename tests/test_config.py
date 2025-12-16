# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import pytest
from molgenis_fdp_harvester.config import validate_config, ConceptTableLink

@pytest.fixture
def valid_config_data():
    return {
        'concept_table_link': {
            'person': 'persons',
            'dataset': 'datasets',
            'datasetseries': 'datasetseries'
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


@pytest.mark.parametrize("missing_concept", ["person", "dataset", "datasetseries"])
def test_validate_config_missing_concept(valid_config_data, missing_concept):
    """Test validation fails when a concept field is missing."""
    invalid_config_data = valid_config_data.copy()
    invalid_config_data['concept_table_link'] = valid_config_data['concept_table_link'].copy()
    del invalid_config_data['concept_table_link'][missing_concept]

    with pytest.raises(ValueError, match="Invalid configuration:"):
        validate_config(invalid_config_data)
