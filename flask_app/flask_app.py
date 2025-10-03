from flask import Flask, request, jsonify
import logging
import os
import sys

# Add project root to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from config.schemas import CONFIG_SCHEMA
from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager

# New service/data layers
from flask_app.services.yield_calc import compute_yields
from flask_app.services.optimizer import optimize_ore_tiered
from flask_app.data.sde_adapter import get_all_ores, get_mineral_list
from flask_app.data.character_repo import get_character_skills, get_implants_for_character
from flask_app.data.facility_repo import get_facility
from flask_app.data.esi_adapter import get_ore_prices, get_mineral_prices  # ADD get_mineral_prices import

#--------------------------------------------------------------------------------------------------
# Initialize configuration and managers
#--------------------------------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
try:
    cfgManager = ConfigManager(base_path="config/config.json", schema=CONFIG_SCHEMA, secret_path="config/secret.json")
    cfg = cfgManager.all()

    db_oauth = DatabaseManager(cfg["app"]["database_oauth_uri"])
    db_app = DatabaseManager(cfg["app"]["database_app_uri"])
    db_sde = DatabaseManager(cfg["app"]["database_sde_uri"])

    char_manager_all = CharacterManager(cfgManager, db_oauth, db_app, db_sde)
    main_character = char_manager_all.get_main_character()
    if not main_character:
        raise ValueError("No main character defined in configuration.")    
except Exception as e:
    logging.error(f"Failed to initialize Flask app: {e}")
    raise e

from flask_app.data import sde_adapter
sde_adapter.init_sde(db_sde)
from flask_app.data.esi_adapter import esi_adapter
esi_adapter(main_character)

#--------------------------------------------------------------------------------------------------
# Helper functions
#--------------------------------------------------------------------------------------------------
# Serialize SQLAlchemy objects to dicts
def serialize_type(type):
    return {
        "id": type.id,
        "groupID": type.groupID,
        "name": type.name.get('en') if type.name else None,
        "portionSize": type.portionSize,
        "volume": type.volume,
        "mass": type.mass,
        "published": type.published,
        "description": type.description.get('en') if type.description else None,
        "graphicID": type.graphicID,
        "iconID": type.iconID,
        "basePrice": type.basePrice,
        "marketGroupID": type.marketGroupID
    }

def get_esi_prices(main_character, type_ids):
    try:
        prices = main_character.esi_client.esi_get("/latest/markets/prices/")

        # Map type_id to average price
        price_map = {item["type_id"]: item.get("average_price", item.get("adjusted_price", 0)) for item in prices if item["type_id"] in type_ids}
        return price_map
    except Exception as e:
        logging.error(f"Error retrieving market prices: {e}")
        return {}

def get_jita_sell_price(type_id):
    region_id = 10000002  # The Forge (Jita)
    station_id = 60003760  # Jita 4-4
    try:
        orders = main_character.esi_client.esi_get(f"/latest/markets/{region_id}/orders/?type_id={type_id}&order_type=sell")

        # Filter for sell orders in Jita 4-4
        jita_orders = [o for o in orders if not o["is_buy_order"] and o["location_id"] == station_id]
        if not jita_orders:
            return None  # No sell orders in Jita 4-4
        lowest_price = min(o["price"] for o in jita_orders)
        return lowest_price
    except Exception as e:
        logging.error(f"Error fetching Jita sell price for type_id {type_id}: {e}")
        return None

def get_jita_depth_price(type_id, required_qty):
    region_id = 10000002  # The Forge (Jita)
    station_id = 60003760  # Jita 4-4
    try:
        orders = main_character.esi_client.esi_get(
            f"/latest/markets/{region_id}/orders/?type_id={type_id}&order_type=sell"
        )
        # Filter for sell orders in Jita 4-4
        jita_orders = [o for o in orders if not o["is_buy_order"] and o["location_id"] == station_id]
        if not jita_orders:
            return None  # No sell orders in Jita 4-4

        # Sort by price ascending
        sorted_orders = sorted(jita_orders, key=lambda o: o["price"])
        qty_accum = 0
        total_cost = 0
        for o in sorted_orders:
            take_qty = min(o["volume_remain"], required_qty - qty_accum)
            total_cost += take_qty * o["price"]
            qty_accum += take_qty
            if qty_accum >= required_qty:
                break
        if qty_accum < required_qty or qty_accum == 0:
            return None  # Not enough volume available
        avg_price = total_cost / qty_accum
        return avg_price
    except Exception as e:
        logging.error(f"Error fetching Jita depth price for type_id {type_id}: {e}")
        return None

#--------------------------------------------------------------------------------------------------
# Flask Endpoints
#--------------------------------------------------------------------------------------------------
@app.route('/restart', methods=['POST'])
def restart():
    # Respond before exiting
    os._exit(0)  # This will terminate the Flask process, main.py will relaunch it
    return "Restarting...", 200

@app.route('/refresh_wallet_balances', methods=['POST'])
def refresh_wallet_balances():
    """
    Endpoint to refresh wallet balances for characters.
    Accepts an optional 'character_name' parameter to refresh a specific character.
    """
    try:
        refreshed_data = char_manager_all.refresh_wallet_balance()
        return jsonify({
            "status": "success",
            "data": refreshed_data
        }), 200
    except Exception as e:
        logging.error(f"Error refreshing wallet balances: {e}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route("/market/prices", methods=["POST"])
def market_prices():
    """
    Body (optional):
    {
      "minerals": ["Tritanium", ...] OR [{id:..., name:...}, ...],
      "ores": [ore_id, ...]
    }
    Returns:
    {
      "minerals": {
         <mineral_name>: {
             "orders": [ {price, volume_remain, min_volume, order_id}, ... ],
             "best_price": <float or null>,
             "total_volume": <int>
         }, ...
      },
      "ores": {
         <ore_id>: [ {price, volume_remain, min_volume, order_id}, ... ]
      }
    }
    """
    try:
        payload = request.get_json(force=True) or {}
        minerals_payload = payload.get("minerals")

        # Build full mineral reference list from SDE (id + name)
        mineral_ref = get_mineral_list()   # [{id, name, ...}]
        name_to_ref = {m["name"]: m for m in mineral_ref}
        id_to_ref = {m["id"]: m for m in mineral_ref}

        # Determine which minerals to price
        if not minerals_payload:
            selected_refs = mineral_ref
        else:
            if isinstance(minerals_payload[0], dict):
                # Expect dicts with id or name
                selected_refs = []
                for d in minerals_payload:
                    if "id" in d:
                        r = id_to_ref.get(d["id"])
                        if r: selected_refs.append(r)
                    elif "name" in d:
                        r = name_to_ref.get(d["name"])
                        if r: selected_refs.append(r)
                # Fallback: ignore unknown
            else:
                # List of names
                selected_refs = [name_to_ref[n] for n in minerals_payload if n in name_to_ref]

        mineral_ids = [m["id"] for m in selected_refs]

        # Fetch full order ladders for minerals (same function as ores)
        mineral_orders_by_id = get_mineral_prices(mineral_ids)  # {id: [orders]}
        # Reshape keyed by mineral name
        minerals_out = {}
        for mid, orders in mineral_orders_by_id.items():
            ref = id_to_ref.get(mid)
            if not ref:
                continue
            # Ensure orders are sorted asc price
            orders_sorted = sorted(orders, key=lambda o: o["price"])
            best_price = orders_sorted[0]["price"] if orders_sorted else None
            total_depth_units = sum(o.get("volume_remain", 0) for o in orders_sorted)
            minerals_out[ref["name"]] = {
                "orders": orders_sorted,
                "best_price": best_price,
                "market_depth_units": total_depth_units,
                "unit_volume": ref.get("volume", 0.01)  # real per-unit volume
            }

        ore_ids = payload.get("ores")
        ore_prices = get_ore_prices(ore_ids) if ore_ids else {}

        return jsonify({"minerals": minerals_out, "ores": ore_prices})
    except Exception as e:
        logging.error(f"/market/prices error: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/reprocessing/yield", methods=["POST"])
def reprocessing_yield():
    """
    Body: {
      "character_id": int,
      "facility_id": int
    }
    """
    try:
        payload = request.get_json(force=True) or {}
        character_id = payload["character_id"]
        facility_id = payload["facility_id"]

        char_skills = get_character_skills(character_id)
        implants = get_implants_for_character(character_id)
        facility = get_facility(facility_id)
        ores = get_all_ores()

        enriched = compute_yields(ores, char_skills, facility, implants)
        return jsonify({"ores": enriched})
    except KeyError as ke:
        return jsonify({"error": f"Missing field {ke}"}), 400
    except Exception as e:
        logging.error(e, exc_info=True)
        return jsonify({"error": str(e)}), 500

BASE_MINERALS = {"Tritanium","Pyerite","Mexallon","Isogen","Nocxium","Zydrine","Megacyte"}

@app.route("/optimize", methods=["POST"])
def optimize():
    """
    Body: {
      "demands": { "Tritanium": qty, ... },
      "character_id": int,
      "facility_id": int,
      "ore_ids": [optional],
      "mode": "min_cost",
      "resale": { "Tritanium": price, ... } (optional),
      "surplus_penalty": float (optional),
      "cost_slack": float (optional)
    }
    """
    try:
        payload = request.get_json(force=True) or {}
        demands = payload["demands"]
        character_id = payload["character_id"]
        facility_id = payload["facility_id"]
        ore_ids = payload.get("ore_ids")
        mode = payload.get("mode", "min_cost")
        resale = payload.get("resale")
        surplus_penalty = float(payload.get("surplus_penalty", 0.0))
        cost_slack = float(payload.get("cost_slack", 0.05))

        exclude_moon = payload.get("exclude_moon_ores", False)
        max_ores = payload.get("max_ores")    # int or None
        sparsity_penalty = payload.get("sparsity_penalty", 0.0)

        # Data
        char_skills = get_character_skills(character_id)
        implants = get_implants_for_character(character_id)
        facility = get_facility(facility_id)
        ores_raw = get_all_ores(ore_ids)
        yields = compute_yields(ores_raw, char_skills, facility, implants)

        # Prepare ores list
        ores_for_opt = []
        minerals = set()
        for o in yields:
            minerals.update(o["batch_yields"].keys())
            ores_for_opt.append({
                "id": o["id"],
                "name": o["name"],
                "portionSize": o["portionSize"],
                "batch_yields": o["batch_yields"]
            })

        # Filter ores if exclude_moon
        if exclude_moon:
            filtered = []
            for o in yields:
                mats = o.get("batch_yields", {})
                # If any produced mineral not in base set -> treat as moon ore and skip
                if any(m not in BASE_MINERALS for m in mats.keys()):
                    continue
                filtered.append(o)
            yields = filtered

        # Build order book & call optimizer (replace previous call)
        order_book = get_ore_prices([o["id"] for o in yields])
        result = optimize_ore_tiered(
            demands=demands,
            ores=[{
                "id": o["id"],
                "name": o["name"],
                "portionSize": o["portionSize"],
                "batch_yields": o["batch_yields"]
            } for o in yields],
            minerals=list(BASE_MINERALS),
            order_book=order_book,
            resale=resale,
            surplus_penalty=surplus_penalty,
            max_ores=max_ores,
            sparsity_penalty=sparsity_penalty
        )
        return jsonify(result), (200 if result.get("status") == "ok" else 400)
    except KeyError as ke:
        return jsonify({"error": f"Missing field {ke}"}), 400
    except Exception as e:
        logging.error(e, exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/minerals", methods=["GET"])
def minerals():
    try:
        data = get_mineral_list()
        return jsonify({"minerals": data})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="localhost", port=5000, debug=True)