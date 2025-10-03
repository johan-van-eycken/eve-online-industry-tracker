def get_character_skills(character_id):
    # Stub; integrate with your CharacterManager/DB later
    return {
        "Refining": 5,
        "Reprocessing Efficiency": 5,
        "Veldspar Processing": 4,
        "Scordite Processing": 4
    }

def get_implants_for_character(character_id):
    return [
        {"slot": 7, "group": "reprocessing", "bonus": 0.04}
    ]