# Validating

> **Note:** To ensure your validator runs smoothly, enable the auto top-up feature for Openrouter, this ensures that your validator will not fail to call synthetic API during task generation. The estimate cost of generating a task is approximately $0.20 USD.

Before starting, create a .env file by making a copy of .env.example

```bash
cp .env.example .env
```

Complete the .env file by changing / uncommenting the required variables

| Variable                | Description                               | Default Value                               | Remarks                                                                                                                                                     |
|-------------------------|-------------------------------------------|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BITTENSOR_DIR           | Bittensor directory                       | $HOME/.bittensor                            |                                                                                                                                                             |
| WALLET_COLDKEY          | Bittensor coldkey name                    | -                                           |                                                                                                                                                             |
| WALLET_HOTKEY           | Bittensor hotkey name                     | -                                           |                                                                                                                                                             |
| NETUID                  | Subnet ID                                 | 52                                          | 52 for mainnet <br>98 for testnet                                                                                                                           |
| SUBTENSOR_NETWORK       | Network name                              | mainnet                                     | finney (mainnet) <br>test (testnet) <br>local (local subtensor)                                                                                             |
| SUBTENSOR_ENDPOINT      | WebSocket endpoint for network connection | <wss://entrypoint-finney.opentensor.ai:443> | <wss://test.finney.opentensor.ai:443> for testnet <br><ws://mainnet-lite:9944> for local subtensor <br><ws://testnet-lite:9944> for local testnet subtensor |
| OPENROUTER_API_KEY      | OpenRouter API authentication key         | -                                           | OpenRouter API key                                                                                                                                          |
| SYNTHETIC_API_URL       | Synthetic API service URL                 | <http://synthetic-api:5003>                 | Internal service endpoint                                                                                                                                   |
| LANGFUSE_SECRET_KEY     | Langfuse secret key                       | -                                           | Langfuse secret key                                                                                                                                         |
| LANGFUSE_PUBLIC_KEY     | Langfuse public key                       | -                                           | Langfuse public key                                                                                                                                         |
| LANGFUSE_HOST           | Langfuse host URL                         | <https://us.cloud.langfuse.com>             | Langfuse endpoint                                                                                                                                           |
| REDIS_HOST              | Redis host                                | redis                                       | Container name or IP                                                                                                                                        |
| REDIS_PORT              | Redis port                                | 6379                                        | Default Redis port                                                                                                                                          |
| DB_HOST                 | Database host address                     | postgres:5432                               | Format: hostname:port                                                                                                                                       |
| DB_NAME                 | Database name                             | db                                          | Database Name                                                                                                                                               |
| DB_USERNAME             | Database username                         | -                                           | Database Username                                                                                                                                           |
| DB_PASSWORD             | Database password                         | -                                           | Database Password                                                                                                                                           |
| VALIDATOR_API_BASE_URL  | Data Collection Endpoint                  | <https://dojo-validator-api.tensorplex.ai>  |                                                                                                                                                             |
| DOJO_LOKI_URL           | Loki Endpoint                             | <https://dojo-logs.tensorplex.ai>           |                                                                                                                                                             |
| VALIDATOR_HOTKEY        | SS58 Address for labelling                | -                                           |                                                                                                                                                             |

Start the validator

```bash
# To start the validator:
make validator
```

To start with autoupdate for validators (**strongly recommended**), see the [Auto-updater](#auto-updater) section.

## Data Collection

To export all data that has been collected from the validator, ensure that you have the environment variables setup properly as in [validator-setup](#validating), then run the following:

```bash
make validator-pull
make extract-dataset
```

# Recommended

## Auto-updates with Watchtower

It is recommended to run watchtower alongside your miner to automatically keep your containers up-to-date. Watchtower monitors running containers and automatically pulls and recreates them when it detects that the image has changed.

To start watchtower:

```bash
make watchtower
```

To stop watchtower:

```bash
make watchtower-down
```
