"""
Adapter for accessing Character data directly.
"""
_character = None

def char_adapter(character):
    global _character
    _character = character

def _ensure():
    if not _character:
        raise RuntimeError("Character not initialized. Call char_adapter(character) first.")

def get_character_skills():
    _ensure()
    return _character.reprocessing_skills or _character.extract_reprocessing_skills()

def get_character_implants(implant_pct: int):
    _ensure()
    return [
        {"slot": 7, "group": "reprocessing", "bonus": (implant_pct / 100)}
    ]

def get_character_assets():
    _ensure()
    return _character.assets or []