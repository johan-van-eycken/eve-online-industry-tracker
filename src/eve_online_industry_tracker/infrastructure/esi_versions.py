# ESI endpoints and the compatibility date that pins their response shapes.
#
# ESI versions by date, not by route: every request carries an
# X-Compatibility-Date header and ESI answers with each route as it was on
# that date. The old /latest/, /dev/, /legacy/ and /vN/ prefixes are legacy
# aliases; ESI ignores them and goes by the header. So the paths below are the
# unversioned routes of the official OpenAPI spec, and ESI_COMPATIBILITY_DATE is
# the one pin. The ESI client and the public ESI service send it on every
# request, whatever config.json says.
#
# Spec:      https://esi.evetech.net/meta/openapi.json?compatibility_date=<date>
# Dates:     https://esi.evetech.net/meta/compatibility-dates
# Changelog: https://esi.evetech.net/meta/changelog
#
# Before moving the date, read the changelog for the routes below. Breaking
# changes after 2025-08-26 that this code would feel:
#   2026-06-09  /characters/{character_id}: 'title' renamed 'corporation_title'.
#   2026-07-21  /corporations/{corporation_id}: 'tax_rate' moved to
#               'tax_rates.isk', 'faction_id' renamed 'enlisted_faction_id',
#               'ceo_id' and 'creator_id' optional.
#
# Usage: call .format(**kwargs) to substitute path parameters, e.g.
#   ESI_CHAR_SKILLS.format(character_id=123456)

ESI_COMPATIBILITY_DATE = "2025-08-26"

# ---------------------------------------------------------------------------
# Market endpoints
# ---------------------------------------------------------------------------
ESI_MARKETS_ORDERS   = "/markets/{region_id}/orders"
ESI_MARKETS_HISTORY  = "/markets/{region_id}/history"
ESI_MARKETS_PRICES   = "/markets/prices"

# ---------------------------------------------------------------------------
# Character endpoints
# ---------------------------------------------------------------------------
ESI_CHAR_PUBLIC            = "/characters/{character_id}"
ESI_CHAR_WALLET            = "/characters/{character_id}/wallet"
ESI_CHAR_WALLET_JOURNAL    = "/characters/{character_id}/wallet/journal"
ESI_CHAR_WALLET_TRANSACTIONS = "/characters/{character_id}/wallet/transactions"
ESI_CHAR_STANDINGS         = "/characters/{character_id}/standings"
ESI_CHAR_SKILLS            = "/characters/{character_id}/skills"
ESI_CHAR_SKILLQUEUE        = "/characters/{character_id}/skillqueue"
ESI_CHAR_IMPLANTS          = "/characters/{character_id}/implants"
ESI_CHAR_ORDERS            = "/characters/{character_id}/orders"
ESI_CHAR_ASSETS            = "/characters/{character_id}/assets"
ESI_CHAR_ASSETS_NAMES      = "/characters/{character_id}/assets/names"
ESI_CHAR_BLUEPRINTS        = "/characters/{character_id}/blueprints"
ESI_CHAR_INDUSTRY_JOBS     = "/characters/{character_id}/industry/jobs"

# ---------------------------------------------------------------------------
# Corporation endpoints
# ---------------------------------------------------------------------------
ESI_CORP_PUBLIC            = "/corporations/{corporation_id}"
ESI_CORP_DIVISIONS         = "/corporations/{corporation_id}/divisions"
ESI_CORP_WALLETS           = "/corporations/{corporation_id}/wallets"
ESI_CORP_WALLETS_JOURNAL   = "/corporations/{corporation_id}/wallets/{division}/journal"
ESI_CORP_WALLETS_TRANSACTIONS = "/corporations/{corporation_id}/wallets/{division}/transactions"
ESI_CORP_STANDINGS         = "/corporations/{corporation_id}/standings"
ESI_CORP_STRUCTURES        = "/corporations/{corporation_id}/structures"
ESI_CORP_MEMBERS           = "/corporations/{corporation_id}/members"
ESI_CORP_MEMBERS_TITLES    = "/corporations/{corporation_id}/members/titles"
ESI_CORP_TITLES            = "/corporations/{corporation_id}/titles"
ESI_CORP_ORDERS            = "/corporations/{corporation_id}/orders"
ESI_CORP_ASSETS            = "/corporations/{corporation_id}/assets"
ESI_CORP_ASSETS_NAMES      = "/corporations/{corporation_id}/assets/names"
ESI_CORP_BLUEPRINTS        = "/corporations/{corporation_id}/blueprints"
ESI_CORP_INDUSTRY_JOBS     = "/corporations/{corporation_id}/industry/jobs"

# ---------------------------------------------------------------------------
# Universe endpoints
# ---------------------------------------------------------------------------
ESI_UNIVERSE_NAMES         = "/universe/names"
ESI_UNIVERSE_TYPE          = "/universe/types/{type_id}"
ESI_UNIVERSE_STATIONS      = "/universe/stations/{station_id}"
ESI_UNIVERSE_STRUCTURES_LIST = "/universe/structures"
ESI_UNIVERSE_STRUCTURE     = "/universe/structures/{structure_id}"
ESI_UNIVERSE_REGIONS       = "/universe/regions/{region_id}"
ESI_UNIVERSE_CONSTELLATIONS = "/universe/constellations/{constellation_id}"
ESI_UNIVERSE_SYSTEMS       = "/universe/systems/{system_id}"

# ---------------------------------------------------------------------------
# Industry endpoints
# ---------------------------------------------------------------------------
ESI_INDUSTRY_FACILITIES    = "/industry/facilities"
ESI_INDUSTRY_SYSTEMS       = "/industry/systems"
