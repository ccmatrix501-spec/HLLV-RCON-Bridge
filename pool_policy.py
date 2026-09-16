from __future__ import annotations

import logging

import rcon_pool_patch as pool

logger = logging.getLogger("hllv-rcon-pool.policy")

# Keep unknown methods on the command lane by default (the safer choice), but
# recognise common read-only naming conventions so future feature modules do not
# accidentally route harmless lookups through the moderation/command connection.
# This can be extended here without changing the pool implementation itself.
_SAFE_READ_PREFIXES = (
    "get_",
    "list_",
    "fetch_",
    "query_",
    "find_",
    "search_",
    "check_",
    "read_",
    "is_",
    "has_",
    "status_",
)

pool._READ_PREFIXES = tuple(dict.fromkeys((*pool._READ_PREFIXES, *_SAFE_READ_PREFIXES)))

logger.info("RCON read routing policy loaded with %s safe prefixes", len(pool._READ_PREFIXES))
