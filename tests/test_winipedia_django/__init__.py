"""__init__ module."""

from winipedia_utils.utils.logging.logger import get_logger

import winipedia_django

logger = get_logger(__name__)

logger.info(
    "Imported django: %s to setup django in tests for winipedia_utils.django",
    winipedia_django,
)
