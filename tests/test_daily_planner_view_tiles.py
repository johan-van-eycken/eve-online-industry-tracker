from streamlit_ui.pages.daily_planner import VIEW_ACTIONS, VIEW_ANALYTICS, VIEW_BUILD, VIEW_SHOPPING, view_tiles


def test_view_tiles_open_the_view_their_step_belongs_to():
    plan = {
        "actions": [
            {"action_type": "deliver", "status": "pending", "character_id": 1},
            {"action_type": "buy_materials", "shopping_category": "invention_input", "status": "pending",
             "estimated_cost_isk": 2_000_000.0},
        ],
        "items": [{"decision": "build"}, {"decision": "skip"}, {"decision": "skip"}],
    }
    tiles = view_tiles(plan)
    assert [(t[0], t[2]) for t in tiles] == [
        ("1 · Deliver", VIEW_ACTIONS), ("2 · Buy materials", VIEW_SHOPPING), ("3 · Start jobs", VIEW_ACTIONS),
        ("Build Plan", VIEW_BUILD), ("Analytics", VIEW_ANALYTICS),
    ]
    assert tiles[1][1] == "1 item(s) · 2.00M ISK"   # invention inputs count as buys too
    assert tiles[3][1] == "1 build · 2 skip"
