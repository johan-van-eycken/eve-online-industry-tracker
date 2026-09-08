# Pinned ESI endpoint versions.
# Update when CCP announces a deprecation — check the ESI changelog on GitHub:
# https://github.com/esi/esi-issues/blob/master/changelog.md
#
# Usage: call .format(**kwargs) to substitute path parameters, e.g.
#   ESI_CHAR_SKILLS.format(character_id=123456)
#
# Stable versions sourced from the ESI public swagger spec (mid-2026).
# Lines marked "# TODO: verify version" use /latest/ as a safe implicit fallback;
# pin them to a numbered version once the stable version is confirmed.

# ---------------------------------------------------------------------------
# Market endpoints
# ---------------------------------------------------------------------------
ESI_MARKETS_ORDERS   = "/v1/markets/{region_id}/orders/"
ESI_MARKETS_HISTORY  = "/v1/markets/{region_id}/history/"
ESI_MARKETS_PRICES   = "/latest/markets/prices/"  # TODO: verify version

# ---------------------------------------------------------------------------
# Character endpoints
# ---------------------------------------------------------------------------
ESI_CHAR_PUBLIC            = "/latest/characters/{character_id}/"          # TODO: verify version
ESI_CHAR_WALLET            = "/latest/characters/{character_id}/wallet/"   # TODO: verify version
ESI_CHAR_WALLET_JOURNAL    = "/latest/characters/{character_id}/wallet/journal/"        # TODO: verify version
ESI_CHAR_WALLET_TRANSACTIONS = "/latest/characters/{character_id}/wallet/transactions/"  # TODO: verify version
ESI_CHAR_STANDINGS         = "/latest/characters/{character_id}/standings/"  # TODO: verify version
ESI_CHAR_SKILLS            = "/v4/characters/{character_id}/skills/"
ESI_CHAR_SKILLQUEUE        = "/latest/characters/{character_id}/skillqueue/"  # TODO: verify version
ESI_CHAR_IMPLANTS          = "/latest/characters/{character_id}/implants/"    # TODO: verify version
ESI_CHAR_ORDERS            = "/latest/characters/{character_id}/orders/"      # TODO: verify version
ESI_CHAR_ASSETS            = "/v5/characters/{character_id}/assets/"
ESI_CHAR_ASSETS_NAMES      = "/latest/characters/{character_id}/assets/names"  # TODO: verify version
ESI_CHAR_BLUEPRINTS        = "/latest/characters/{character_id}/blueprints/"   # TODO: verify version
ESI_CHAR_INDUSTRY_JOBS     = "/v1/characters/{character_id}/industry/jobs/"

# ---------------------------------------------------------------------------
# Corporation endpoints
# ---------------------------------------------------------------------------
ESI_CORP_PUBLIC            = "/latest/corporations/{corporation_id}/"              # TODO: verify version
ESI_CORP_DIVISIONS         = "/latest/corporations/{corporation_id}/divisions/"    # TODO: verify version
ESI_CORP_WALLETS           = "/v1/corporations/{corporation_id}/wallets/"
ESI_CORP_WALLETS_JOURNAL   = "/latest/corporations/{corporation_id}/wallets/{division}/journal/"       # TODO: verify version
ESI_CORP_WALLETS_TRANSACTIONS = "/latest/corporations/{corporation_id}/wallets/{division}/transactions/"  # TODO: verify version
ESI_CORP_STANDINGS         = "/latest/corporations/{corporation_id}/standings/"    # TODO: verify version
ESI_CORP_STRUCTURES        = "/latest/corporations/{corporation_id}/structures/"   # TODO: verify version
ESI_CORP_MEMBERS           = "/latest/corporations/{corporation_id}/members/"      # TODO: verify version
ESI_CORP_MEMBERS_TITLES    = "/latest/corporations/{corporation_id}/members/titles/"  # TODO: verify version
ESI_CORP_TITLES            = "/latest/corporations/{corporation_id}/titles/"       # TODO: verify version
ESI_CORP_ORDERS            = "/latest/corporations/{corporation_id}/orders/"       # TODO: verify version
ESI_CORP_ASSETS            = "/v5/corporations/{corporation_id}/assets/"
ESI_CORP_ASSETS_NAMES      = "/latest/corporations/{corporation_id}/assets/names"  # TODO: verify version
ESI_CORP_BLUEPRINTS        = "/latest/corporations/{corporation_id}/blueprints/"   # TODO: verify version
ESI_CORP_INDUSTRY_JOBS     = "/v1/corporations/{corporation_id}/industry/jobs/"

# ---------------------------------------------------------------------------
# Universe endpoints
# ---------------------------------------------------------------------------
ESI_UNIVERSE_NAMES         = "/v3/universe/names/"
ESI_UNIVERSE_TYPE          = "/v3/universe/types/{type_id}/"
ESI_UNIVERSE_STATIONS      = "/latest/universe/stations/{station_id}/"       # TODO: verify version
ESI_UNIVERSE_STRUCTURES_LIST = "/latest/universe/structures/"                # TODO: verify version
ESI_UNIVERSE_STRUCTURE     = "/latest/universe/structures/{structure_id}/"   # TODO: verify version
ESI_UNIVERSE_REGIONS       = "/latest/universe/regions/{region_id}/"         # TODO: verify version
ESI_UNIVERSE_CONSTELLATIONS = "/latest/universe/constellations/{constellation_id}/"  # TODO: verify version
ESI_UNIVERSE_SYSTEMS       = "/latest/universe/systems/{system_id}/"         # TODO: verify version

# ---------------------------------------------------------------------------
# Industry endpoints
# ---------------------------------------------------------------------------
ESI_INDUSTRY_FACILITIES    = "/latest/industry/facilities/"   # TODO: verify version
ESI_INDUSTRY_SYSTEMS       = "/latest/industry/systems/"      # TODO: verify version
