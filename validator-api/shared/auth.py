from bittensor.utils.btlogging import logging as logger
from fastapi import HTTPException, Request

from commons.utils import check_stake, verify_hotkey_in_metagraph, verify_signature


class ValidatorAuth:
    """Authentication handler for validator operations"""

    @staticmethod
    async def validate_validator(
        request: Request,
        hotkey: str,
        signature: str,
        message: str,
    ) -> None:
        """
        Validate a validator's credentials and permissions

        Args:
            request: FastAPI request object containing app state
            hotkey: Validator's hotkey
            signature: Signature of the message
            message: Original message that was signed

        Raises:
            HTTPException: If validation fails
        """
        logger.info(f"Validating credentials for hotkey: {hotkey}")

        # Verify signature format and validity
        if not signature.startswith("0x"):
            logger.error(f"Invalid signature format for hotkey: {hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature format")

        if not verify_signature(hotkey, signature, message):
            logger.error(f"Invalid signature for hotkey: {hotkey}")
            raise HTTPException(status_code=401, detail="Invalid signature")

        # Verify hotkey in metagraph
        if not verify_hotkey_in_metagraph(request.app.state.metagraph, hotkey):
            logger.error(f"Hotkey {hotkey} not found in metagraph")
            raise HTTPException(status_code=401, detail="Hotkey not found in metagraph")

        # Verify stake
        if not check_stake(request.app.state.subtensor, hotkey):
            logger.error(f"Insufficient stake for hotkey {hotkey}")
            raise HTTPException(status_code=401, detail="Insufficient stake for hotkey")

        logger.info(f"Successfully validated credentials for hotkey: {hotkey}")
