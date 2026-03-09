# CLI tests
# Test that if only the required parameters are supplied, that we have a workable program.
# Test that the dotenv is picked up correctly
# Test that the correct harvester is created in create_harvester, and that ValueError is raised if the 'else' branch is triggered.

import io
import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock, call
from click.testing import CliRunner

from molgenis_fdp_harvester.harvester import cli, create_harvester, read_fdp_list
from molgenis_emx2_pyclient import Client


@pytest.fixture
def temp_config_file():
    """Create a temporary config file"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        f.write("""[concept_table_link]
dataset = "collections"
datasetseries = "biobanks"
kind = "kind"
publisher = "publisher"
provenancestatement = "provenancestatement"

[harvester_config]
auto_create_datasetseries = true
""")
        config_path = f.name

    yield config_path

    # Cleanup
    os.unlink(config_path)


@pytest.fixture
def base_cli_args(temp_config_file):
    """Common CLI arguments for testing"""
    return [
        '--fdp', 'http://example.com/fdp',
        '--host', 'http://localhost:8080',
        '--config', temp_config_file,
        '--input_type', 'rdf'
    ]


@pytest.fixture
def concept_table_dict():
    """Standard concept table dictionary for testing"""
    return {
        'dataset': 'collections',
        'datasetseries': 'biobanks',
        'kind': 'kind',
        'publisher': 'publisher',
        'provenancestatement': 'provenancestatement'
    }


@pytest.fixture
def harvester_config():
    """Standard harvester config for testing"""
    return {'auto_create_datasetseries': True}


@pytest.fixture
def mock_harvester_patches():
    """Mock all harvester-related components for CLI testing"""
    with patch('molgenis_fdp_harvester.harvester.Client') as mock_client_class, \
         patch('molgenis_fdp_harvester.harvester.create_harvester') as mock_create_harvester, \
         patch('molgenis_fdp_harvester.harvester.execute_harvest') as mock_execute_harvest:

        # Configure mock client context manager
        mock_client_instance = MagicMock(spec=Client)
        mock_client_class.return_value.__enter__.return_value = mock_client_instance
        mock_client_class.return_value.__exit__.return_value = None

        # Configure mock harvester
        mock_harvester = Mock()
        mock_create_harvester.return_value = mock_harvester

        yield {
            'client_class': mock_client_class,
            'client_instance': mock_client_instance,
            'create_harvester': mock_create_harvester,
            'execute_harvest': mock_execute_harvest,
            'harvester': mock_harvester
        }


def test_dotenv_token_pickup(base_cli_args, mock_harvester_patches, monkeypatch):
    """Test that MOLGENIS_TOKEN from environment is properly picked up by the CLI"""
    runner = CliRunner()

    # Set environment variable before invoking CLI
    # This simulates what load_dotenv() does when reading a .env file
    monkeypatch.setenv('MOLGENIS_TOKEN', 'test_token_from_env_file')

    # Invoke CLI without --token parameter
    # The lambda default will evaluate os.environ.get("MOLGENIS_TOKEN") at call time
    result = runner.invoke(cli, base_cli_args + ['--schema', 'Eucaim'])

    # Verify the command completed successfully
    assert result.exit_code == 0, f"CLI failed with exit code {result.exit_code}:\nOutput: {result.output}"

    # Verify that Client was instantiated with the token from environment
    mock_harvester_patches['client_class'].assert_called_once()
    call_kwargs = mock_harvester_patches['client_class'].call_args.kwargs

    # The token should be picked up from the environment variable
    assert call_kwargs['token'] == 'test_token_from_env_file', \
        f"Token not correctly picked up from environment. Got: {call_kwargs.get('token')}"

    # Verify other parameters
    assert call_kwargs['url'] == 'http://localhost:8080'
    assert call_kwargs['schema'] == 'Eucaim'


def test_dotenv_token_explicit_override(base_cli_args, mock_harvester_patches, monkeypatch):
    """Test that explicit --token parameter overrides environment variable"""
    runner = CliRunner()

    # Set environment variable
    monkeypatch.setenv('MOLGENIS_TOKEN', 'test_token_from_env_file')

    # Invoke CLI with explicit --token parameter (should override environment)
    result = runner.invoke(cli, base_cli_args + [
        '--schema', 'Eucaim',
        '--token', 'explicit_token_override'
    ])

    # Verify command completed successfully
    assert result.exit_code == 0, f"CLI failed with: {result.output}"

    # Verify that Client was instantiated with the explicit token (not from environment)
    mock_harvester_patches['client_class'].assert_called_once()
    call_kwargs = mock_harvester_patches['client_class'].call_args.kwargs

    assert call_kwargs['token'] == 'explicit_token_override', \
        f"Explicit token not used. Got: {call_kwargs.get('token')}"


def test_missing_token_raises_error(base_cli_args, monkeypatch):
    """Test that CLI raises an error when no token is provided"""
    runner = CliRunner()

    # Ensure MOLGENIS_TOKEN is not set in environment
    monkeypatch.delenv('MOLGENIS_TOKEN', raising=False)

    # Invoke CLI without --token parameter and without environment variable
    result = runner.invoke(cli, base_cli_args + ['--schema', 'Eucaim'])

    # Verify the command failed with appropriate error
    assert result.exit_code != 0, "CLI should have failed when no token is provided"
    assert "Authentication token is required" in result.output, \
        f"Expected error message about missing token. Got: {result.output}"


def test_cli_with_only_required_parameters(base_cli_args, mock_harvester_patches, monkeypatch):
    """Test that CLI works when only required parameters are supplied with MOLGENIS_TOKEN set"""
    runner = CliRunner()

    # Set environment variable to simulate .env file
    monkeypatch.setenv('MOLGENIS_TOKEN', 'test_token_from_env')

    # Invoke CLI with ONLY required parameters (no optional --token or --schema)
    # --schema has a default value of "Eucaim", so we test without explicitly providing it
    result = runner.invoke(cli, base_cli_args)

    # Verify the command completed successfully
    assert result.exit_code == 0, \
        f"CLI should work with only required parameters. Exit code: {result.exit_code}, Output: {result.output}"

    # Verify that create_harvester was called, indicating the program initialized correctly
    mock_harvester_patches['create_harvester'].assert_called_once()
    call_args = mock_harvester_patches['create_harvester'].call_args

    # Verify correct input_type was passed
    assert call_args[0][0] == 'rdf', "Input type should be 'rdf'"

    # Verify that execute_harvest was called, indicating the harvesting process started
    mock_harvester_patches['execute_harvest'].assert_called_once()
    harvest_call_args = mock_harvester_patches['execute_harvest'].call_args

    # Verify the FDP URL was passed correctly
    assert harvest_call_args[0][1] == 'http://example.com/fdp', \
        "FDP URL should be passed to execute_harvest"

    # Verify Client was instantiated with correct parameters
    mock_harvester_patches['client_class'].assert_called_once()
    client_kwargs = mock_harvester_patches['client_class'].call_args.kwargs

    assert client_kwargs['url'] == 'http://localhost:8080', "Host URL should be correct"
    assert client_kwargs['schema'] == 'Eucaim', "Default schema should be 'Eucaim'"
    assert client_kwargs['token'] == 'test_token_from_env', \
        "Token should be picked up from environment"


def test_cli_all_env_vars(temp_config_file, mock_harvester_patches, monkeypatch):
    """CLI works when all arguments are supplied via environment variables, with no CLI flags."""
    runner = CliRunner()

    monkeypatch.setenv('MOLGENIS_TOKEN', 'env_token')
    monkeypatch.setenv('MOLGENIS_HOST', 'http://env-host:8080')
    monkeypatch.setenv('MOLGENIS_SCHEMA', 'EnvSchema')
    monkeypatch.setenv('HARVEST_CONFIG', temp_config_file)
    monkeypatch.setenv('INPUT_TYPE', 'rdf')
    monkeypatch.setenv('FDP_URL', 'http://env-fdp.example.com')

    result = runner.invoke(cli, [])

    assert result.exit_code == 0, f"CLI failed with exit code {result.exit_code}:\nOutput: {result.output}"

    mock_harvester_patches['client_class'].assert_called_once()
    client_kwargs = mock_harvester_patches['client_class'].call_args.kwargs
    assert client_kwargs['url'] == 'http://env-host:8080'
    assert client_kwargs['schema'] == 'EnvSchema'
    assert client_kwargs['token'] == 'env_token'

    mock_harvester_patches['create_harvester'].assert_called_once()
    assert mock_harvester_patches['create_harvester'].call_args[0][0] == 'rdf'

    mock_harvester_patches['execute_harvest'].assert_called_once()
    assert mock_harvester_patches['execute_harvest'].call_args[0][1] == 'http://env-fdp.example.com'


@pytest.mark.parametrize("input_type,expected_class", [
    ('rdf', 'DCATRDFHarvester'),
    ('fdp', 'FDPHarvester'),
])
def test_create_harvester_valid_types(input_type, expected_class, concept_table_dict, harvester_config):
    """Test that create_harvester returns the correct harvester type for 'rdf' and 'fdp'"""
    mock_client = Mock(spec=Client)

    harvester = create_harvester(input_type, concept_table_dict, mock_client, harvester_config)

    assert type(harvester).__name__ == expected_class, \
        f"Expected {expected_class}, got {type(harvester).__name__}"


def test_create_harvester_invalid_type(concept_table_dict, harvester_config):
    """Test that create_harvester raises ValueError for invalid input_type"""
    mock_client = Mock(spec=Client)

    with pytest.raises(ValueError) as exc_info:
        create_harvester('invalid', concept_table_dict, mock_client, harvester_config)

    assert "Unknown input_type" in str(exc_info.value), \
        f"Expected error message about unknown input_type, got: {exc_info.value}"


# ---------------------------------------------------------------------------
# Multi-FDP CSV tests
# ---------------------------------------------------------------------------

@pytest.fixture
def temp_config_file_no_header():
    """Config file with fdp_list_has_header = false"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        f.write("""[concept_table_link]
dataset = "collections"
datasetseries = "biobanks"
kind = "kind"
publisher = "publisher"
provenancestatement = "provenancestatement"

[harvester_config]
auto_create_datasetseries = true
fdp_list_has_header = false
""")
        config_path = f.name

    yield config_path

    os.unlink(config_path)


@pytest.fixture
def temp_fdp_list_with_header():
    """CSV file with header row and two FDP entries"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("fdp_url,fdp_id_prefix\n")
        f.write("http://fdp1.example.com,prefix1\n")
        f.write("http://fdp2.example.com,prefix2\n")
        csv_path = f.name

    yield csv_path

    os.unlink(csv_path)


@pytest.fixture
def temp_fdp_list_without_header():
    """CSV file without header row and two FDP entries"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("http://fdp1.example.com,prefix1\n")
        f.write("http://fdp2.example.com,prefix2\n")
        csv_path = f.name

    yield csv_path

    os.unlink(csv_path)


def test_fdp_and_fdp_list_mutually_exclusive(temp_config_file, temp_fdp_list_with_header, monkeypatch):
    """Providing both --fdp and --fdp-list should fail with exit code 2"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    result = runner.invoke(cli, [
        '--fdp', 'http://example.com',
        '--fdp-list', temp_fdp_list_with_header,
        '--host', 'http://localhost:8080',
        '--config', temp_config_file,
        '--input_type', 'rdf',
    ])

    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_neither_fdp_nor_fdp_list_raises_error(temp_config_file, monkeypatch):
    """Providing neither --fdp nor --fdp-list should fail with exit code 2"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    result = runner.invoke(cli, [
        '--host', 'http://localhost:8080',
        '--config', temp_config_file,
        '--input_type', 'rdf',
    ])

    assert result.exit_code == 2
    assert "--fdp" in result.output or "required" in result.output.lower()


def test_fdp_list_with_header(temp_config_file, temp_fdp_list_with_header, mock_harvester_patches, monkeypatch):
    """CSV with header row: execute_harvest called once per data row"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    result = runner.invoke(cli, [
        '--fdp-list', temp_fdp_list_with_header,
        '--host', 'http://localhost:8080',
        '--config', temp_config_file,
        '--input_type', 'fdp',
    ])

    assert result.exit_code == 0, f"Unexpected failure: {result.output}"
    assert mock_harvester_patches['execute_harvest'].call_count == 2

    calls = mock_harvester_patches['execute_harvest'].call_args_list
    urls = [c[0][1] for c in calls]
    assert 'http://fdp1.example.com' in urls
    assert 'http://fdp2.example.com' in urls


def test_fdp_list_without_header(temp_config_file_no_header, temp_fdp_list_without_header, mock_harvester_patches, monkeypatch):
    """CSV without header row: first row treated as data, execute_harvest called twice"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    result = runner.invoke(cli, [
        '--fdp-list', temp_fdp_list_without_header,
        '--host', 'http://localhost:8080',
        '--config', temp_config_file_no_header,
        '--input_type', 'fdp',
    ])

    assert result.exit_code == 0, f"Unexpected failure: {result.output}"
    assert mock_harvester_patches['execute_harvest'].call_count == 2


def test_fdp_list_per_row_prefix(temp_config_file, temp_fdp_list_with_header, mock_harvester_patches, monkeypatch):
    """Each CSV row's fdp_id_prefix is passed to create_harvester as entry_config"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    result = runner.invoke(cli, [
        '--fdp-list', temp_fdp_list_with_header,
        '--host', 'http://localhost:8080',
        '--config', temp_config_file,
        '--input_type', 'fdp',
    ])

    assert result.exit_code == 0, f"Unexpected failure: {result.output}"
    assert mock_harvester_patches['create_harvester'].call_count == 2

    calls = mock_harvester_patches['create_harvester'].call_args_list
    prefixes = [c[0][3].get('fdp_id_prefix') for c in calls]
    assert 'prefix1' in prefixes
    assert 'prefix2' in prefixes


def test_fdp_list_row_without_prefix(temp_config_file, mock_harvester_patches, monkeypatch):
    """A CSV row with no prefix column should not set fdp_id_prefix in config"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("fdp_url,fdp_id_prefix\n")
        f.write("http://fdp1.example.com,\n")  # blank prefix
        csv_path = f.name

    try:
        result = runner.invoke(cli, [
            '--fdp-list', csv_path,
            '--host', 'http://localhost:8080',
            '--config', temp_config_file,
            '--input_type', 'fdp',
        ])

        assert result.exit_code == 0, f"Unexpected failure: {result.output}"
        call_args = mock_harvester_patches['create_harvester'].call_args
        entry_config = call_args[0][3]
        assert 'fdp_id_prefix' not in entry_config
    finally:
        os.unlink(csv_path)


def test_single_fdp_backward_compat(base_cli_args, mock_harvester_patches, monkeypatch):
    """--fdp still works and execute_harvest is called exactly once"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    result = runner.invoke(cli, base_cli_args)

    assert result.exit_code == 0, f"Unexpected failure: {result.output}"
    assert mock_harvester_patches['execute_harvest'].call_count == 1
    call_url = mock_harvester_patches['execute_harvest'].call_args[0][1]
    assert call_url == 'http://example.com/fdp'


# ---------------------------------------------------------------------------
# Unit tests for read_fdp_list helper
# ---------------------------------------------------------------------------

def test_read_fdp_list_with_header(tmp_path):
    """read_fdp_list correctly skips header and returns data rows"""
    csv_file = tmp_path / "fdps.csv"
    csv_file.write_text("fdp_url,fdp_id_prefix\nhttp://a.com,pA\nhttp://b.com,pB\n")

    result = read_fdp_list(csv_file, has_header=True)

    assert result == [('http://a.com', 'pA'), ('http://b.com', 'pB')]


def test_read_fdp_list_without_header(tmp_path):
    """read_fdp_list treats first row as data when has_header=False"""
    csv_file = tmp_path / "fdps.csv"
    csv_file.write_text("http://a.com,pA\nhttp://b.com,pB\n")

    result = read_fdp_list(csv_file, has_header=False)

    assert result == [('http://a.com', 'pA'), ('http://b.com', 'pB')]


def test_read_fdp_list_missing_prefix_column(tmp_path):
    """read_fdp_list returns None for prefix when column is absent or blank"""
    csv_file = tmp_path / "fdps.csv"
    csv_file.write_text("fdp_url,fdp_id_prefix\nhttp://a.com,\nhttp://b.com\n")

    result = read_fdp_list(csv_file, has_header=True)

    assert result == [('http://a.com', None), ('http://b.com', None)]


def test_read_fdp_list_skips_blank_rows(tmp_path):
    """read_fdp_list skips entirely blank rows"""
    csv_file = tmp_path / "fdps.csv"
    csv_file.write_text("fdp_url,fdp_id_prefix\nhttp://a.com,pA\n\nhttp://b.com,pB\n")

    result = read_fdp_list(csv_file, has_header=True)

    assert result == [('http://a.com', 'pA'), ('http://b.com', 'pB')]


def test_fdp_list_empty_raises_error(temp_config_file, monkeypatch):
    """An --fdp-list file with no valid entries should fail with an error"""
    runner = CliRunner()
    monkeypatch.setenv('MOLGENIS_TOKEN', 'token')

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("fdp_url,fdp_id_prefix\n")  # header only, no data rows
        csv_path = f.name

    try:
        result = runner.invoke(cli, [
            '--fdp-list', csv_path,
            '--host', 'http://localhost:8080',
            '--config', temp_config_file,
            '--input_type', 'fdp',
        ])

        assert result.exit_code != 0
        assert "no valid entries" in result.output.lower() or "no valid entries" in str(result.exception).lower()
    finally:
        os.unlink(csv_path)
