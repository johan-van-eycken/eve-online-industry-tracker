from typing import TypedDict, NotRequired

class CharacterRow(TypedDict):
    character_name: str
    character_id: int
    refresh_token: str
    is_main: bool

# TypedDict voor de input characters
class CharacterConfig(TypedDict):
    character_name: str
    is_main: NotRequired[bool]