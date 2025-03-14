PRECOMMIT_VERSION="3.7.1"
UNAME := $(shell uname)
.PHONY: hooks install install-dev install-test btcli validator-pull miner-pull miner-decentralised miner-centralised validator validator-up-deps miner-worker-api dojo-cli miner-decentralised-logs miner-centralised-logs validator-logs

hooks:
	@echo "Grabbing pre-commit version ${PRECOMMIT_VERSION} and installing pre-commit hooks"
	if [ ! -f pre-commit.pyz ]; then \
		wget -O pre-commit.pyz https://github.com/pre-commit/pre-commit/releases/download/v${PRECOMMIT_VERSION}/pre-commit-${PRECOMMIT_VERSION}.pyz; \
	fi
	python3 pre-commit.pyz clean
	python3 pre-commit.pyz uninstall --hook-type pre-commit --hook-type pre-push --hook-type commit-msg
	python3 pre-commit.pyz gc
	python3 pre-commit.pyz install --hook-type pre-commit --hook-type pre-push --hook-type commit-msg

# ---------------------------------------------------------------------------- #
#                           INSTALL DEPS & UTILITIES                           #
# ---------------------------------------------------------------------------- #

install:
	@if [ "$(UNAME)" = "Darwin" ]; then \
		pip install -e .; \
	elif [ "$(UNAME)" = "Linux" ]; then \
		pip install -e . --find-links https://download.pytorch.org/whl/torch_stable.html; \
	fi

install-dev:
	@if [ "$(UNAME)" = "Darwin" ]; then \
		pip install -e ".[dev]"; \
	elif [ "$(UNAME)" = "Linux" ]; then \
		pip install -e ".[dev]" --find-links https://download.pytorch.org/whl/torch_stable.html; \
	fi

install-test:
	@if [ "$(UNAME)" = "Darwin" ]; then \
		pip install -e ".[test]"; \
	elif [ "$(UNAME)" = "Linux" ]; then \
		pip install -e ".[test]" --find-links https://download.pytorch.org/whl/torch_stable.html; \
	fi

btcli:
	docker compose -f docker-compose.shared.yaml run --rm btcli

validator-pull:
	docker compose -f docker-compose.validator.yaml pull --include-deps

miner-pull:
	docker compose -f docker-compose.miner.yaml pull --include-deps

validator-down:
	docker compose -f docker-compose.validator.yaml down

miner-down:
	docker compose -f docker-compose.miner.yaml down

# ---------------------------------------------------------------------------- #
#                                 CORE SERVICES                                #
# ---------------------------------------------------------------------------- #

miner:
	docker compose -f docker-compose.miner.yaml up -d miner

validator:
	docker compose -f docker-compose.validator.yaml up -d validator

validator-up-deps:
	docker compose -f docker-compose.validator.yaml up -d --build synthetic-api postgres prisma-setup-vali

dojo-platform:
	docker compose -f docker-compose.platform.yaml up -d

dojo-cli:
	docker compose -f docker-compose.miner.yaml run --rm dojo-cli

extract-dataset:
	docker compose -f docker-compose.validator.yaml run --remove-orphans extract-dataset

fill-score-column:
	docker compose -f docker-compose.validator.yaml run --remove-orphans fill-score-column

migration:
	docker compose -f docker-compose.validator.yaml run --rm migration

# ---------------------------------------------------------------------------- #
#                             CORE SERVICE LOGGING                             #
# ---------------------------------------------------------------------------- #

miner-logs:
	docker compose -f docker-compose.miner.yaml logs --since=1h -f miner

validator-logs:
	docker compose -f docker-compose.validator.yaml logs --since=1h -f validator

# ---------------------------------------------------------------------------- #
#                             LOCAL SUBTENSOR                                  #
# ---------------------------------------------------------------------------- #

subtensor-mainnet:
	docker compose -f docker-compose.subtensor.yaml up -d mainnet-lite

subtensor-testnet:
	docker compose -f docker-compose.subtensor.yaml up -d testnet-lite

# ---------------------------------------------------------------------------- #
#                             WORKER PLATFORM                                  #
# ---------------------------------------------------------------------------- #

worker-platform:
	docker compose -f docker-compose.platform.yaml up -d

# ---------------------------------------------------------------------------- #
#                                  OTHERS                                      #
# ---------------------------------------------------------------------------- #

watchtower:
	docker compose -f docker-compose.shared.yaml up -d watchtower

watchtower-down:
	docker compose -f docker-compose.shared.yaml down watchtower
