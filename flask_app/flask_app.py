from unittest import result
from flask import Flask, request, jsonify
import logging
import os
import sys
import traceback

from flask_app.data.char_adapter import char_adapter

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
from flask_app.data.sde_adapter import get_all_ores, get_all_materials
from flask_app.data.char_adapter import get_character_skills, get_character_implants
from flask_app.data.facility_repo import get_facility, get_all_facilities
from flask_app.data.esi_adapter import get_ore_prices, get_material_prices  # ADD get_material_prices import

from utils.ore_calculator_core import filter_viable_ores

REGION_ID = 10000002  # The Forge (Jita)
STATION_ID = 60003760  # Jita 4-4
MATERIALS = None

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

from flask_app.data.sde_adapter import sde_adapter
sde_adapter(db_sde)

from flask_app.data.esi_adapter import esi_adapter
esi_adapter(main_character)

#--------------------------------------------------------------------------------------------------
# Flask Endpoints
#--------------------------------------------------------------------------------------------------
# ADMINISTRATOR endpoints
@app.route('/restart', methods=['POST'])
def restart():
    # Respond before exiting
    os._exit(0)  # This will terminate the Flask process, main.py will relaunch it
    return "Restarting...", 200

# Characters endpoints
@app.route('/refresh_wallet_balances', methods=['POST'])
def refresh_wallet_balances():
    """
    Endpoint to refresh wallet balances for all characters.
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

# Ore Calculator endpoints
@app.route("/facilities", methods=["GET"])
def facilities():
    """
    Get all facilities.
    """
    try:
        facilities = get_all_facilities()
        return jsonify(facilities), 200
    except Exception as e:
        logging.error(f"Error in /facilities: {e}")
        return jsonify({"error": str(e), "facilities": []}), 500

@app.route("/optimize", methods=["POST"])
def optimize():
    """
    Body: {
      "demands": { "Tritanium": qty, ... },
      "character_id": int,
      "facility_id": int,
      "ore_ids": [optional],
      "mode": "min_cost"
    }
    """
    try:
        payload = request.get_json(force=True) or {}
        demands = payload["demands"]
        character_id = payload["character_id"]
        facility_id = payload["facility_id"]
        mode = payload.get("mode", "min_cost")
        opt_only_compressed = payload.get("only_compressed", False)

        # 1. Get character skills
        character = char_manager_all.get_character_by_id(character_id)
        if not character:
            return jsonify({"error": f"Character ID {character_id} not found"}), 400
        
        char_adapter(character)
        skills = get_character_skills()
        implants = get_character_implants()
        
        # 2. Get facility
        facility = get_facility(facility_id)

        # 3. Get all ores and compute yields
        ores = get_all_ores()
        ore_yields = compute_yields(ores, skills, facility, implants)

        # 4. Keep ores that yield only a subset of the requested materials (no extra materials)
        req_mats = set(demands.keys())
        ore_yields = [
            o for o in ore_yields
            if set(o["batch_yields"].keys()).issubset(req_mats)
            and len(o["batch_yields"].keys()) > 0
        ]

        # 5. Fetch market prices for the required materials and ores
        materials = get_all_materials()
        req_mat_ids = [m['id'] for m in materials if m['name'] in req_mats]
        raw_req_mat_prices = get_material_prices(req_mat_ids)
        req_mat_prices = {
            m: raw_req_mat_prices.get(m, [{}])[0].get("price", None)
            for m in req_mat_ids
            if m in raw_req_mat_prices and raw_req_mat_prices[m]
        }

        # --- ADD THIS BLOCK: Calculate tiered_total_cost ---
        # Map material name to price for easy lookup
        mat_name_to_price = {}
        for m in materials:
            if m['name'] in req_mats:
                mat_name_to_price[m['name']] = req_mat_prices.get(m['id'], None)

        tiered_total_cost = 0.0
        for mat, qty in demands.items():
            price = None
            # Try to get price by name or by id
            if mat in mat_name_to_price:
                price = mat_name_to_price[mat]
            else:
                # fallback: try to get by id if mat is id
                price = req_mat_prices.get(mat, None)
            if price is not None:
                tiered_total_cost += qty * price

        # 6. Viability filtering: Ensure at least one material is cheaper than market price
        # viable_ores = filter_viable_ores(ore_yields, req_mat_prices, skills, facility["base_yield"], batch_size=1, strict=False, slack=0.5)
        viable_ores = [
            o for o in ore_yields
            if any(m in req_mats for m in o["batch_yields"].keys())
            and len(o["batch_yields"].keys()) > 0
        ]
        if opt_only_compressed:
            viable_compressed_ores = [o for o in viable_ores if "Compressed" in o["name"]]
            viable_ores = viable_compressed_ores
            

        # 7. Build order book for optimizer
        ore_ids = [o["id"] for o in viable_ores]
        processed_ore_prices = get_ore_prices(ore_ids)
        order_book = {oid: processed_ore_prices.get(oid, []) for oid in ore_ids}

        # 8. Run optimizer
        result = optimize_ore_tiered(
            demands=demands,
            ores=viable_ores,
            materials=req_mats,
            order_book=order_book,
            max_ore_types=len(req_mats)
        )
        # Compute total ore volume and yielded materials
        total_ore_volume = 0
        total_yielded_materials = {mat: 0 for mat in req_mats}

        for ore_sol in result.get("solution", []):
            total_ore_volume += ore_sol["ore_units"]
            # Find the ore yield info
            ore_yield = next((o for o in ore_yields if o["id"] == ore_sol["ore_id"]), None)
            if ore_yield:
                for mat, qty_per_batch in ore_yield["batch_yields"].items():
                    total_yielded_materials[mat] += qty_per_batch * ore_sol["batches"]
        
        result["ore_yields"] = ore_yields
        result["total_ore_volume"] = total_ore_volume
        result["total_yielded_materials"] = total_yielded_materials
        result["tiered_total_cost"] = tiered_total_cost

        total_ore_volume_m3 = 0
        for ore_sol in result.get("solution", []):
            ore_yield = next((o for o in ore_yields if o["id"] == ore_sol["ore_id"]), None)
            if ore_yield:
                total_ore_volume_m3 += ore_sol["batches"] * ore_yield["batch_volume"]
        result["total_ore_volume_m3"] = total_ore_volume_m3

        # Build mineral_volumes dict before using it
        mineral_volumes = {m['name']: m['volume'] for m in materials if m['name'] in req_mats}

        raw_comparator = []
        for mat, qty in demands.items():
            price = None
            # Try to get price by name or by id
            if mat in mat_name_to_price:
                price = mat_name_to_price[mat]
            else:
                price = req_mat_prices.get(mat, None)
            volume_per_unit = mineral_volumes.get(mat, 0)
            total_volume = qty * volume_per_unit
            if price is not None:
                raw_comparator.append({
                    "Mineral": mat,
                    "Quantity": qty,
                    "Unit Price": price,
                    "Total Cost": qty * price,
                    "Total Volume": total_volume
                })

        total_raw_volume = sum(qty * mineral_volumes.get(mat, 0) for mat, qty in demands.items())
        result["total_raw_volume"] = total_raw_volume

        # Demand coverage details
        demand_coverage = {}
        for mat in demands:
            demand_coverage[mat] = {
                "demand": demands[mat],
                "yielded": total_yielded_materials.get(mat, 0),
                "surplus": result["surplus"].get(mat, 0),
                "shortfall": max(0, demands[mat] - total_yielded_materials.get(mat, 0))
            }
        result["demand_coverage"] = demand_coverage

        # Raw material comparator and surplus initialization
        result["raw_comparator"] = raw_comparator
        result["surplus"] = result.get("surplus", {})
        
        # After you have total_yielded_materials and demands
        surplus = {}
        for mat, demand in demands.items():
            yielded = total_yielded_materials.get(mat, 0)
            surplus[mat] = max(0, yielded - demand)

        result["surplus"] = surplus

        return jsonify(result), (200 if result.get("status") == "ok" else 400)
    except KeyError as ke:
        print("Error in /optimize:", traceback.format_exc())
        return jsonify({"error": f"Missing field {ke}"}), 400
    except Exception as e:
        print("Error in /optimize:", traceback.format_exc())
        logging.error(e, exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/materials", methods=["GET"])
def materials():
    global MATERIALS
    try:
        if MATERIALS is None:
            MATERIALS = get_all_materials()
        return jsonify({"materials": MATERIALS})
    except Exception as e:
        print("Error in /materials:", traceback.format_exc())
        return jsonify({"error": str(e), "materials": []}), 500

@app.route("/ores", methods=["GET"])
def ores():
    try:
        ores = get_all_ores()
        return ores
    except Exception as e:
        print("Error in /ores:", traceback.format_exc())
        return jsonify({"error": str(e), "ores": []}), 500


if __name__ == "__main__":
    app.run(host="localhost", port=5000, debug=True)