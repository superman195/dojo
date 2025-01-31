import os
import platform
import shlex

import nox
from dotenv import load_dotenv

SUPPORTED_PYTHON_VERSIONS = ["3.10", "3.11", "3.12"]
VENV_BACKEND = "uv|conda|venv"
REUSE_VENV = True
# specify tests under a single session, otherwise "compatibility" and "unit_tests" will use different venvs
VENV_NAME = "tests_session"

nox.options.reuse_existing_virtualenvs = True


def get_install_cmd():
    system = platform.system().lower()
    if system == "linux":
        return shlex.split(
            "-e .[test] --find-links https://download.pytorch.org/whl/torch_stable.html"
        )
    elif system == "darwin":
        return shlex.split("-e .[test]")
    else:
        raise ValueError(f"Unsupported platform: {system}")


@nox.session(
    python=SUPPORTED_PYTHON_VERSIONS,
    venv_backend=VENV_BACKEND,
    reuse_venv=REUSE_VENV,
    name=VENV_NAME,
)
def compatibility(session):
    session.install(
        *get_install_cmd(),
        silent=False,
    )
    pip_show_output = session.run("pip", "show", "dojo", silent=True)
    if "not found" in pip_show_output:
        raise Exception(
            "Missing dojo package, this means installation probably failed."
        )


@nox.session(
    python=SUPPORTED_PYTHON_VERSIONS,
    venv_backend=VENV_BACKEND,
    reuse_venv=REUSE_VENV,
    name=VENV_NAME,
)
def unit_tests(session):
    session.install(
        *get_install_cmd(),
        silent=False,
    )

    def run_tests_with_env(env_file, test_file):
        load_dotenv(env_file)
        # inject environment variables into the session
        for key, value in os.environ.items():
            session.env[key] = value

        session.run("pytest", "-s", "-v", f"tests/unit_testing/{test_file}")

    # run miner tests with .env.miner
    run_tests_with_env(".env.miner", "test_miner.py")

    # run validator tests with .env.validator
    # run_tests_with_env(".env.validator", "test_validator.py")

    other_tests = [
        f
        for f in os.listdir("tests/unit_testing")
        if f.startswith("test_")
        and f
        not in [
            "test_miner.py",
            "test_validator.py",
        ]
    ]
    if other_tests:
        session.run(
            "pytest", "-s", "-v", *[f"tests/unit_testing/{f}" for f in other_tests]
        )
