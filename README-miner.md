# Mining
There are two options in setting up a miner for Dojo (centralised method and decentralised method)

Before starting, create a .env file by making a copy of .env.example

```bash
cp .env.example .env
```

### Option 1: Centralised Method
Complete the .env file by changing / uncommenting the required variables

| Variable            | Description                                                       | Default Value                               | Remarks                                                                                                                                                     |
|---------------------|-------------------------------------------------------------------|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BITTENSOR_DIR       | Bittensor directory                                               | $HOME/.bittensor                            |                                                                                                                                                             |
| WALLET_COLDKEY      | Bittensor coldkey name                                            | -                                           |                                                                                                                                                             |
| WALLET_HOTKEY       | Bittensor hotkey name                                             | -                                           |                                                                                                                                                             |
| NETUID              | Subnet ID                                                         | 52                                          | 52 for mainnet <br>98 for testnet                                                                                                                           |
| SUBTENSOR_NETWORK   | Network name                                                      | mainnet                                     | finney (mainnet) <br>test (testnet) <br>local (local subtensor)                                                                                             |
| SUBTENSOR_ENDPOINT  | WebSocket endpoint for network connection                         | <wss://entrypoint-finney.opentensor.ai:443> | <wss://test.finney.opentensor.ai:443> for testnet <br><ws://mainnet-lite:9944> for local subtensor <br><ws://testnet-lite:9944> for local testnet subtensor |
| DOJO_API_BASE_URL   | Base URL for Dojo API                                             | <https://dojo-api.tensorplex.ai>            | Dojo Worker API URL                                                                                                                                         |
| DOJO_API_KEY        | Authentication key for Dojo API                                   | -                                           | Dojo API key (Generate it in the next step)                                                                                                                 |
| AXON_PORT           | Port for Axon server                                              | 8091                                        |                                                                                                                                                             |
| VALIDATOR_MIN_STAKE | Optional minimum stake requirement                                | 20000                                       | It is recommended to set value to 0 for testnet                                                                                                             |
| TASK_MAX_RESULTS    | The number of workers that may submit responses for a single task | 4                                           |                                                                                                                                                             |

Run Dojo CLI to retrieve API Key and Subscription Key. See [Dojo CLI](#dojo-cli) for usage. Note down the API key, subscription key and append the API key to your .env file.

> You can use tab completions to see a list of commands

```bash
make dojo-cli

# Authenticate and generate keys
authenticate
api_key generate
subscription_key generate

# List all keys
api_key list
subscription_key list
```

Start the miner by running the following commands

```bash
make miner
```

### Option 2: Decentralised Method

Complete the .env file by changing / uncommenting the required variables

| Variable                | Description                   | Default Value                                          | Remarks                                 |
|-------------------------|-------------------------------|--------------------------------------------------------|-----------------------------------------|
| SAS_SUBSTRATE_URL       | Substrate URL                 | Same as SUBTENSOR_ENDPOINT                             | Must match network configuration        |
| SAS_EXPRESS_PORT        | Sidecar Express server port   | 8081                                                   | Internal service port                   |
| SUBSTRATE_API_URL       | Substrate API URL             | sidecar:8081                                           | Internal service endpoint               |
| NEXT_PUBLIC_BACKEND_URL | Backend URL for Dojo UI       | <http://localhost:3000>                                | Must be accessible from UI              |
| SERVER_PORT             | Worker API server port        | 8080                                                   | Must not conflict with other services   |
| RUNTIME_ENV             | Runtime environment           | aws                                                    | Options: local, development, production |
| CORS_ALLOWED_ORIGINS    | Allowed CORS origins          | <http://localhost*,http://worker-ui*,http://dojo-cli>* | Comma-separated list                    |
| TOKEN_EXPIRY            | JWT token expiration in hours | 24                                                     | Adjust if needed                        |
| JWT_SECRET              | Secret key for JWT tokens     | -                                                      | Use a strong random string              |
| AWS_ACCESS_KEY_ID       | AWS access key ID             | -                                                      | Optional                                |
| AWS_SECRET_ACCESS_KEY   | AWS secret access key         | -                                                      | Optional                                |
| AWS_S3_BUCKET_NAME      | S3 bucket name                | -                                                      | Optional                                |
| S3_PUBLIC_URL           | Public URL for S3 bucket      | -                                                      | Optional                                |
| REDIS_HOST              | Redis host                    | redis                                                  | Container name or IP                    |
| REDIS_PORT              | Redis port                    | 6379                                                   | Default Redis port                      |
| REDIS_USERNAME          | Redis username                | -                                                      | (Optional) For Redis ACL                |
| REDIS_PASSWORD          | Redis password                | -                                                      | (Optional) For Redis authentication     |
| DB_HOST                 | Database host address         | postgres:5432                                          | Format: hostname:port                   |
| DB_NAME                 | Database name                 | db                                                     | Database Name                           |
| DB_USERNAME             | Database username             | -                                                      | Database Username                       |
| DB_PASSWORD             | Database password             | -                                                      | Database Password                       |
| VALIDATOR_MIN_STAKE     | Validator Minimum Stake       | 20000                                                  | Minimum stake required from validators  |
| ETHEREUM_NODE           | Ethereum Node endpoint        | <https://ethereum.publicnode.com>                      | Ethereum endpoint                       |
Start the dojo platform which Dojo CLI will interact with later.

```bash
make dojo-platform
```

Refer to [option 1](#option-1-centralised-method) to continue setting up the miner.

### Setup Subscription Key for Labellers on UI to connect to Dojo Subnet for scoring

Note: URLs are different for testnet and mainnet. Please refer to [docs](https://docs.tensorplex.ai/tensorplex-docs/tensorplex-dojo-testnet/official-links).

1. Head to https://dojo.tensorplex.ai or https://dojo-testnet.tensorplex.ai and login and sign with your Metamask wallet.

- You'll see an empty homepage with no Tasks, and a "Connect" button on the top right ![image](./assets/ui/homepage.png)
- Click on "Connect" and you'll see a popup with different wallets for you to connect to ![image](./assets/ui/wallet_popup.jpg)
- Click "Next" and "Continue", then finally it will be requesting a signature from your wallet, please sign and it will be connected. ![image](./assets/ui/wallet_sign.jpg)
- Once connected, the top navigation bar should display your wallet address. ![image](./assets/ui/wallet_connected.png)

2. Once connected, please stay connected to your wallet and click on "Enter Subscription Key". ![image](./assets/subscription/enter_subscription.png)

- Give your subscription a name, and enter your subscription key generated earlier before running the miner. _*Refer to step 4 of "Getting Started" if you need to retrieve your key*_ ![image](./assets/subscription/enter_details.png)
- Click "Create" and your subscription will be saved. ![image](./assets/subscription/created_details.png)
- Confirmed your subscription is created properly, and that you can view your tasks! ![image](./assets/subscription/tasks_shown.png)

# Dojo CLI

We provide a CLI that allows miners to manage their API and subscription keys either when connecting to our hosted Tensorplex API services or their own self-hosted miner backend.

Features:

- Tab completion
- Prefix matching wallets

You may use the dockerized version of the CLI using

```bash
make dojo-cli
```

Alternatively you can simply run the CLI inside of a virtual environment

```bash
# Start the dojo cli tool
# Upon starting the CLI it will ask if you wanna use the default path for bittensor wallets, which is `~/.bittensor/wallets/`.
# If you want to use a different path, please enter 'n' and then specify the path when prompted.
dojo

# TIP: During the whole process, you could actually use tab-completion to display the options, so you don't have to remember them all. Please TAB your way guys! 🙇‍♂️
# It should be prompting you to enter you coldkey and hotkey
# After entering the coldkey and hotkey, you should be in the command line interface for dojo, please authenticate by running the following command.
# You should see a message saying "✅ Wallet coldkey name and hotkey name set successfully."
authenticate

# Afterwards, please generate an API Key with the following command.
# You should see a message saying:  "✅ All API keys: ['sk-<KEY>]". Displaying a list of your API Keys.
api_key generate

# Lastly, please generate a Subscription Key with the following command.
# You should see a message saying:  "✅ All Subscription keys: ['sk-<KEY>]". Displaying a list of your Subscription Keys.
subscription_key generate

# :rocket: You should now have all the required keys, and be able to start mining.

# Other commands available to the CLI:
# You can always run the following command to get your current keys.
api_key list
subscription_key list

# You can also delete your keys with the following commands.
api_key delete
subscription_key delete
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
