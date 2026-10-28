from types import SimpleNamespace

from sqlalchemy.orm import sessionmaker

from eve_online_industry_tracker.application.locations.service import LocationsService
from eve_online_industry_tracker.infrastructure.models import PublicStructuresModel, StructureNameCacheModel

CACHED, PUBLIC, UNKNOWN, STATION = 1_030_000_000_001, 1_030_000_000_002, 1_030_000_000_003, 60003760


class _Esi:
    def __init__(self):
        self.calls = []

    def get_location_info(self, location_id, **kwargs):
        self.calls.append(location_id)
        return {"name": "Jita IV - Moon 4"} if location_id == STATION else None


class _Char:
    character_name = "Pilot"

    def __init__(self, calls):
        self.esi_client = SimpleNamespace(esi_get=lambda endpoint, **kw: calls.append(endpoint))

    def ensure_esi(self):
        pass


def test_a_known_structure_is_answered_from_the_db_without_any_esi_call(app_engine):
    session = sessionmaker(bind=app_engine, future=True)()
    session.add(StructureNameCacheModel(structure_id=CACHED, name="Cached Keepstar"))
    session.add(PublicStructuresModel(structure_id=PUBLIC, system_id=30000142, structure_name="Public Fortizar"))
    session.commit()
    esi, char_calls = _Esi(), []
    state = SimpleNamespace(
        esi_service=esi,
        db_app=SimpleNamespace(session=session),
        char_manager=SimpleNamespace(get_characters=lambda: [_Char(char_calls)]),
    )
    service = LocationsService(state=state)

    result = service.get_locations([CACHED, PUBLIC, STATION, UNKNOWN])

    assert result[str(CACHED)]["name"] == "Cached Keepstar"
    assert result[str(PUBLIC)]["name"] == "Public Fortizar"
    assert result[str(STATION)]["name"] == "Jita IV - Moon 4"
    assert result[str(UNKNOWN)] == {}
    assert esi.calls == [STATION, UNKNOWN]          # ESI only for the station and the miss
    assert len(char_calls) == 1                     # and the per-pilot fallback only for the miss
