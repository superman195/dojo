import json
from pathlib import Path

import torch
from bittensor.btlogging import logging as logger

from database.client import connect_db, disconnect_db
from database.prisma.models import Score_Model


class ScoreStorage:
    """Handles persistence of validator scores"""

    SCORES_DIR = Path("scores")
    SCORES_FILE = SCORES_DIR / "miner_scores.pt"

    @classmethod
    async def migrate_from_db(cls) -> bool:
        """One-time migration of scores from database to .pt file
        Returns:
            bool: True if migration successful or file already exists, False if migration failed
        """
        try:
            if cls.SCORES_FILE.exists():
                logger.info("Scores file already exists, skipping migration")
                return True

            # Connect to database first
            await connect_db()

            try:
                # Get scores from database
                score_record = await Score_Model.prisma().find_first(
                    order={"created_at": "desc"}
                )
                if not score_record:
                    logger.warning("No scores found in database to migrate")
                    return True  # Not an error, just no scores yet

                scores = torch.tensor(json.loads(score_record.score))

                # Create scores directory if it doesn't exist
                cls.SCORES_DIR.mkdir(exist_ok=True)

                # Save scores to .pt file
                torch.save(scores, cls.SCORES_FILE)
                logger.success(f"Successfully migrated scores to {cls.SCORES_FILE}")

                # Verify the migration
                loaded_scores = torch.load(cls.SCORES_FILE)
                if torch.equal(scores, loaded_scores):
                    logger.success("Migration verification successful - scores match")
                    return True
                else:
                    logger.error("Migration verification failed - scores do not match")
                    return False

            finally:
                await disconnect_db()

        except Exception as e:
            logger.error(f"Failed to migrate scores: {e}")
            return False

    @classmethod
    async def save(cls, scores: torch.Tensor) -> None:
        """Save validator scores to .pt file"""
        try:
            cls.SCORES_DIR.mkdir(exist_ok=True)
            torch.save(scores, cls.SCORES_FILE)
            logger.success("Successfully saved validator scores to file")
        except Exception as e:
            logger.error(f"Failed to save validator scores: {e}")
            raise

    @classmethod
    async def load(cls) -> torch.Tensor | None:
        """Load validator scores from .pt file"""
        try:
            if not cls.SCORES_FILE.exists():
                logger.warning("No validator scores file found")
                return None

            scores = torch.load(cls.SCORES_FILE)
            logger.success("Successfully loaded validator scores from file")
            return scores
        except Exception as e:
            logger.error(f"Failed to load validator scores: {e}")
            return None
