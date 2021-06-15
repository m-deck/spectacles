from unittest.mock import patch, Mock
import logging
import pytest
import requests
from tests.constants import ENV_VARS
from tests.utils import build_validation
from spectacles.cli import main, create_parser, handle_exceptions
from spectacles.exceptions import (
    LookerApiError,
    SpectaclesException,
    GenericValidationError,
)


@pytest.fixture
def clean_env(monkeypatch):
    for variable in ENV_VARS.keys():
        monkeypatch.delenv(variable, raising=False)


@pytest.fixture
def env(monkeypatch):
    for variable, value in ENV_VARS.items():
        monkeypatch.setenv(variable, value)


@pytest.fixture
def limited_env(monkeypatch):
    for variable, value in ENV_VARS.items():
        if variable in ["LOOKER_CLIENT_SECRET", "LOOKER_PROJECT"]:
            monkeypatch.delenv(variable, raising=False)
        else:
            monkeypatch.setenv(variable, value)


@patch("sys.argv", new=["spectacles", "--help"])
def test_help():
    with pytest.raises(SystemExit) as cm:
        main()
        assert cm.value.code == 0


@pytest.mark.parametrize(
    "exception,exit_code",
    [(ValueError, 1), (SpectaclesException, 100), (GenericValidationError, 102)],
)
def test_handle_exceptions_unhandled_error(exception, exit_code):
    @handle_exceptions
    def raise_exception():
        if exception == SpectaclesException:
            raise exception(
                name="exception-name",
                title="An exception occurred.",
                detail="Couldn't handle the truth. Please try again.",
            )
        elif exception == GenericValidationError:
            raise GenericValidationError
        else:
            raise exception(f"This is a {exception.__class__.__name__}.")

    with pytest.raises(SystemExit) as pytest_error:
        raise_exception()

    assert pytest_error.value.code == exit_code


def test_handle_exceptions_looker_error_should_log_response_and_status(caplog):
    caplog.set_level(logging.DEBUG)
    response = Mock(spec=requests.Response)
    response.request = Mock(spec=requests.PreparedRequest)
    response.request.url = "https://api.looker.com"
    response.request.method = "GET"
    response.json.return_value = {
        "message": "Not found",
        "documentation_url": "http://docs.looker.com/",
    }
    status = 404

    @handle_exceptions
    def raise_exception():
        raise LookerApiError(
            name="exception-name",
            title="An exception occurred.",
            detail="Couldn't handle the truth. Please try again.",
            status=status,
            response=response,
        )

    with pytest.raises(SystemExit) as pytest_error:
        raise_exception()
    captured = "\n".join(record.msg for record in caplog.records)
    assert str(status) in captured
    assert '"message": "Not found"' in captured
    assert '"documentation_url": "http://docs.looker.com/"' in captured
    assert pytest_error.value.code == 101


def test_parse_args_with_no_arguments_supplied(clean_env, capsys):
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["connect"])
    captured = capsys.readouterr()
    assert (
        "the following arguments are required: --base-url, --client-id, --client-secret"
        in captured.err
    )


def test_parse_args_with_one_argument_supplied(clean_env, capsys):
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["connect", "--base-url", "BASE_URL_CLI"])
    captured = capsys.readouterr()
    assert (
        "the following arguments are required: --client-id, --client-secret"
        in captured.err
    )


def test_parse_args_with_only_cli(clean_env):
    parser = create_parser()
    args = parser.parse_args(
        [
            "connect",
            "--base-url",
            "BASE_URL_CLI",
            "--client-id",
            "CLIENT_ID_CLI",
            "--client-secret",
            "CLIENT_SECRET_CLI",
        ]
    )
    assert args.base_url == "BASE_URL_CLI"
    assert args.client_id == "CLIENT_ID_CLI"
    assert args.client_secret == "CLIENT_SECRET_CLI"


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_parse_args_with_only_config_file(mock_parse_config, clean_env):
    parser = create_parser()
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    args = parser.parse_args(["connect", "--config-file", "config.yml"])
    assert args.base_url == "BASE_URL_CONFIG"
    assert args.client_id == "CLIENT_ID_CONFIG"
    assert args.client_secret == "CLIENT_SECRET_CONFIG"


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_parse_args_with_incomplete_config_file(mock_parse_config, clean_env, capsys):
    parser = create_parser()
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
    }
    with pytest.raises(SystemExit):
        parser.parse_args(["connect", "--config-file", "config.yml"])
    captured = capsys.readouterr()
    assert "the following arguments are required: --client-secret" in captured.err


def test_parse_args_with_only_env_vars(env):
    parser = create_parser()
    args = parser.parse_args(["connect"])
    assert args.base_url == "BASE_URL_ENV_VAR"
    assert args.client_id == "CLIENT_ID_ENV_VAR"
    assert args.client_secret == "CLIENT_SECRET_ENV_VAR"


def test_parse_args_with_incomplete_env_vars(limited_env, capsys):
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["connect"])
    captured = capsys.readouterr()
    assert "the following arguments are required: --client-secret" in captured.err


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_arg_precedence(mock_parse_config, limited_env):
    parser = create_parser()
    # Precedence: command line > environment variables > config files
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
    }
    args = parser.parse_args(
        ["connect", "--config-file", "config.yml", "--base-url", "BASE_URL_CLI"]
    )
    assert args.base_url == "BASE_URL_CLI"
    assert args.client_id == "CLIENT_ID_ENV_VAR"
    assert args.client_secret == "CLIENT_SECRET_CONFIG"


def test_env_var_override_argparse_default(env):
    parser = create_parser()
    args = parser.parse_args(["connect"])
    assert args.port == 8080


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_config_override_argparse_default(mock_parse_config, clean_env):
    parser = create_parser()
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "client_id": "CLIENT_ID_CONFIG",
        "client_secret": "CLIENT_SECRET_CONFIG",
        "port": 8080,
    }
    args = parser.parse_args(["connect", "--config-file", "config.yml"])
    assert args.port == 8080


@patch("spectacles.cli.YamlConfigAction.parse_config")
def test_bad_config_file_parameter(mock_parse_config, clean_env):
    parser = create_parser()
    mock_parse_config.return_value = {
        "base_url": "BASE_URL_CONFIG",
        "api_key": "CLIENT_ID_CONFIG",
        "port": 8080,
    }
    with pytest.raises(
        SpectaclesException, match="Invalid configuration file parameter"
    ):
        parser.parse_args(["connect", "--config-file", "config.yml"])


def test_parse_remote_reset_with_assert(env):
    parser = create_parser()
    args = parser.parse_args(["assert", "--remote-reset"])
    assert args.remote_reset


def test_parse_args_with_mutually_exclusive_args_remote_reset(env, capsys):
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["sql", "--commit-ref", "abc123", "--remote-reset"])
    captured = capsys.readouterr()
    assert (
        "argument --remote-reset: not allowed with argument --commit-ref"
        in captured.err
    )


def test_parse_args_with_mutually_exclusive_args_commit_ref(env, capsys):
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["sql", "--remote-reset", "--commit-ref", "abc123"])
    captured = capsys.readouterr()
    assert (
        "argument --commit-ref: not allowed with argument --remote-reset"
        in captured.err
    )


@patch("sys.argv", new=["spectacles", "sql"])
@patch("spectacles.cli.Runner")
@patch("spectacles.cli.tracking")
def test_main_with_sql_validator(mock_tracking, mock_runner, env, caplog):
    validation = build_validation("sql")
    mock_runner.return_value.validate_sql.return_value = validation
    with pytest.raises(SystemExit):
        main()
    mock_tracking.track_invocation_start.assert_called_once_with(
        "BASE_URL_ENV_VAR", "sql", project="PROJECT_ENV_VAR"
    )
    # TODO: Uncomment the below assertion once #262 is fixed
    # mock_tracking.track_invocation_end.assert_called_once()
    mock_runner.assert_called_once_with(
        "BASE_URL_ENV_VAR",  # base_url
        "PROJECT_ENV_VAR",  # project
        "BRANCH_ENV_VAR",  # branch
        "CLIENT_ID_ENV_VAR",  # client_id
        "CLIENT_SECRET_ENV_VAR",  # client_secret
        8080,  # port
        3.1,  # api_version
        False,  # remote_reset
        False,  # import_projects
        None,  # commit_ref
    )
    assert "ecommerce.orders passed" in caplog.text
    assert "ecommerce.sessions passed" in caplog.text
    assert "ecommerce.users failed" in caplog.text


@patch("sys.argv", new=["spectacles", "content"])
@patch("spectacles.cli.Runner")
@patch("spectacles.cli.tracking")
def test_main_with_content_validator(mock_tracking, mock_runner, env, caplog):
    validation = build_validation("content")
    mock_runner.return_value.validate_content.return_value = validation
    with pytest.raises(SystemExit):
        main()
    mock_tracking.track_invocation_start.assert_called_once_with(
        "BASE_URL_ENV_VAR", "content", project="PROJECT_ENV_VAR"
    )
    # TODO: Uncomment the below assertion once #262 is fixed
    # mock_tracking.track_invocation_end.assert_called_once()
    mock_runner.assert_called_once_with(
        "BASE_URL_ENV_VAR",  # base_url
        "PROJECT_ENV_VAR",  # project
        "BRANCH_ENV_VAR",  # branch
        "CLIENT_ID_ENV_VAR",  # client_id
        "CLIENT_SECRET_ENV_VAR",  # client_secret
        8080,  # port
        3.1,  # api_version
        False,  # remote_reset
        False,  # import_projects
        None,  # commit_ref
    )
    assert "ecommerce.orders passed" in caplog.text
    assert "ecommerce.sessions passed" in caplog.text
    assert "ecommerce.users failed" in caplog.text


# @patch("sys.argv", new=["spectacles", "assert"])
# @patch("spectacles.cli.Runner", autospec=True)
# @patch("spectacles.cli.tracking")
# def test_main_with_assert_validator(mock_tracking, mock_runner, env, caplog):
#     validation = build_validation("assert")
#     mock_runner.return_value.validate_data_tests.return_value = validation
#     with pytest.raises(SystemExit):
#         main()
#     mock_tracking.track_invocation_start.assert_called_once_with(
#         "BASE_URL_ENV_VAR", "assert", project="PROJECT_ENV_VAR"
#     )
#     # TODO: Uncomment the below assertion once #262 is fixed
#     # mock_tracking.track_invocation_end.assert_called_once()
#     mock_runner.assert_called_once_with(
#         "BASE_URL_ENV_VAR",  # base_url
#         "PROJECT_ENV_VAR",  # project
#         "BRANCH_ENV_VAR",  # branch
#         "CLIENT_ID_ENV_VAR",  # client_id
#         "CLIENT_SECRET_ENV_VAR",  # client_secret
#         8080,  # port
#         3.1,  # api_version
#         False,  # remote_reset
#         False,  # import_projects
#         None,  # commit_ref
#     )
#     assert "ecommerce.orders passed" in caplog.text
#     assert "ecommerce.sessions passed" in caplog.text
#     assert "ecommerce.users failed" in caplog.text


@patch("sys.argv", new=["spectacles", "connect"])
@patch("spectacles.cli.run_connect")
@patch("spectacles.cli.tracking")
def test_main_with_connect(mock_tracking, mock_run_connect, env):
    main()
    mock_tracking.track_invocation_start.assert_called_once_with(
        "BASE_URL_ENV_VAR", "connect", project=None
    )
    mock_tracking.track_invocation_end.assert_called_once()
    mock_run_connect.assert_called_once_with(
        "BASE_URL_ENV_VAR",  # base_url
        "CLIENT_ID_ENV_VAR",  # client_id
        "CLIENT_SECRET_ENV_VAR",  # client_secret
        8080,  # port
        3.1,  # api_version
    )


@patch("sys.argv", new=["spectacles", "connect", "--do-not-track"])
@patch("spectacles.cli.run_connect")
@patch("spectacles.cli.tracking")
def test_main_with_do_not_track(mock_tracking, mock_run_connect, env):
    main()
    mock_tracking.track_invocation_start.assert_not_called()
    mock_tracking.track_invocation_end.assert_not_called()
    mock_run_connect.assert_called_once_with(
        "BASE_URL_ENV_VAR",  # base_url
        "CLIENT_ID_ENV_VAR",  # client_id
        "CLIENT_SECRET_ENV_VAR",  # client_secret
        8080,  # port
        3.1,  # api_version
    )
