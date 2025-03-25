import sys

from pydantic_settings import CliApp

from dojo.settings import Settings

try:
    """Returns the configuration object specific to this miner or validator after adding relevant arguments."""
    # NOTE: "--help" will not work from CLI as bittensor will override the global argparser
    cli_args = sys.argv[1:] or ["--help"]
    print(sys.argv)
    if "--help-custom" in cli_args:
        # lmao
        cli_args.remove("--help-custom")
        cli_args.append("--help")

    settings: Settings = CliApp.run(Settings, cli_args=cli_args)
    print(f"Settings: {settings.model_dump()}")
except SystemExit:
    pass
