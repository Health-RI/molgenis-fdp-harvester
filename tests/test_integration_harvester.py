# SPDX-FileCopyrightText: 2024 Stichting Health-RI
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import json
import os

import pytest


@pytest.fixture
def catalog_url():
    url = os.path.abspath("tests/test_data/rdf_testdata.ttl")
    assert os.path.exists(url), f"Test file not found: {url}"
    return url

def test_full_harvest_flow(harvester, mock_client, catalog_url, concept_table_dict):
    """Test the complete harvest flow from gather to import"""
    # Reset mock client
    mock_client.reset_mock()

    # Step 1: Gather stage
    harvest_objects = harvester.gather_stage(catalog_url)
    expected_guids = ["http://example.com/dataset1", "http://example.com/dataset2"]

    # Verify gather results
    assert harvest_objects is not None
    assert len(harvest_objects) > 0, "No harvest objects were gathered"

    # Verify guids_in_harvest contains the expected datasets
    for guid in expected_guids:
        assert guid in harvester.guids_in_harvest['dataset'], \
        f"Expected guid {guid} not found in guids_in_harvest"

    # Step 2: Fetch stage - process each harvest object
    processed_objects = []
    for obj in harvest_objects:
        fetched_obj = harvester.fetch_stage(obj)
        assert fetched_obj.content is not None, f"Content not fetched for {obj.guid}"
        processed_objects.append(fetched_obj)

    # Step 3: Import stage - import each harvest object
    import_results = []
    for obj in processed_objects:
        result = harvester.import_stage(obj)
        import_results.append(result)

    # Verify content was saved to Molgenis
    data = json.loads(obj.content)
    mock_client.save_table.assert_any_call(
        table=concept_table_dict['dataset'],
        data=[data]
    )

    expected_save_calls = len(processed_objects)

    # Verify all imports were successful
    assert all(import_results), "Not all imports were successful"

    # Verify expected number of calls to save_table
    assert mock_client.save_table.call_count == expected_save_calls, \
        "Unexpected number of save_table calls"
