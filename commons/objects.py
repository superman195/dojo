from dojo.settings import get_config, parse_cli_config


class ObjectManager:
    _miner = None
    _validator = None
    _seed_dataset_iter = None
    _config = None

    @classmethod
    async def get_miner(cls):
        if get_config().simulation:
            from simulator.miner import MinerSim

            if cls._miner is None:
                # TODO: might need to be async
                cls._miner = MinerSim()
        else:
            from neurons.miner import Miner

            if cls._miner is None:
                cls._miner = await Miner()
        return cls._miner

    @classmethod
    def get_validator(cls):
        if get_config().simulation:
            from simulator.validator import ValidatorSim

            if cls._validator is None:
                cls._validator = ValidatorSim()
        else:
            from neurons.validator import Validator

            if cls._validator is None:
                cls._validator = Validator()
        return cls._validator

    @classmethod
    def get_config(cls):
        parse_cli_config()
        if cls._config is None:
            cls._config = get_config()
        return cls._config
