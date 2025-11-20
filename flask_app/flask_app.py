from datetime import datetime, timedelta, timezone
from flask import Flask, request, jsonify
import logging
import os
import sys
import json

# Add project root to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

# App initialization imports
from utils.app_init import load_config, init_db_managers, init_char_manager, init_corp_manager

# Flask app imports
from flask_app.services.yield_calc import compute_yields
from flask_app.services.optimizer import optimize_ore_tiered
from flask_app.data.sde_adapter import sde_adapter, get_all_ores, get_all_materials
from flask_app.data.char_adapter import char_adapter, get_character_skills, get_character_implants
from flask_app.data.facility_repo import get_facility, get_all_facilities
from flask_app.data.esi_adapter import esi_adapter, get_ore_prices, get_material_prices, \
    get_type_sellprices, get_type_buyprices, get_location_info

from utils.ore_calculator_core import filter_viable_ores

INIT_STATE = None
MATERIALS = None

#--------------------------------------------------------------------------------------------------
# Initialize configuration and managers
#--------------------------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)
INIT_STATE = "Starting Initialization"
try:
    # Main app initializations
    logging.info("Loading config...")
    INIT_STATE = "Loading Config"
    cfgManager = load_config()
    logging.info("Initializing databases...")
    INIT_STATE = "Initializing Databases"
    db_oauth, db_app, db_sde = init_db_managers(cfgManager, refresh_metadata=True)
    logging.info("Initializing characters...")
    INIT_STATE = "Initializing Characters"
    char_manager = init_char_manager(cfgManager, db_oauth, db_app, db_sde)
    char_manager.refresh_all()
    main_character = char_manager.get_main_character()
    logging.info("Initializing corporations...")
    INIT_STATE = "Initializing Corporations"
    corp_manager = init_corp_manager(cfgManager, db_oauth, db_app, db_sde, char_manager)
    corp_manager.refresh_all()

    # Flask Specific: Initialize data adapters
    logging.info("Initializing data adapters...")
    INIT_STATE = "Initializing Data Adapters"
    sde_adapter(db_sde)
    esi_adapter(main_character)

    # Log summary
    chars_initialized = len(char_manager.character_list)
    corps_initialized = len(corp_manager.corporation_ids)
    logging.info(f"All done. Characters: {chars_initialized}, Corporations: {corps_initialized}")
    INIT_STATE = "Ready"
except Exception as e:
    logging.error(f"Failed to initialize application: {e}", exc_info=True)
    INIT_STATE = f"Initialization Failed at step: {INIT_STATE}"
    raise e

#--------------------------------------------------------------------------------------------------
# Flask Endpoints
#--------------------------------------------------------------------------------------------------
# ADMINISTRATOR endpoints
@app.route('/health', methods=['GET'])
def health_check():
    if INIT_STATE != "Ready":
        return jsonify({"status": "not_ready", "init_state": INIT_STATE}), 500
    return jsonify({"status": "OK"}), 200

@app.route('/shutdown', methods=['GET'])
def shutdown():
    # Respond before exiting
    os._exit(0)  # This will terminate the Flask process
    return jsonify({"status": "Shutting down..."}), 200

# Static Data endpoints
@app.route('/static/<path:filename>', methods=['GET'])
def get_static_file(filename):
    return app.send_static_file(filename)

# Characters endpoints
@app.route('/refresh_wallet_balances', methods=['GET'])
def refresh_wallet_balances():
    """
    Endpoint to refresh wallet balances for all characters.
    """
    try:
        refreshed_data = char_manager.refresh_wallet_balance()
        return jsonify({"status": "success", "data": refreshed_data}), 200
    except Exception as e:
        logging.error(f"Error refreshing wallet balances: {e}")
        return jsonify({"status": "error", "message": f"Error in GET Method `/refresh_wallet_balances`: " + str(e)}), 500

@app.route('/refresh_assets', methods=['GET'])
def refresh_assets():
    """
    Endpoint to refresh assets for all characters.
    """
    try:
        refreshed_data = char_manager.refresh_assets()
        return jsonify({"status": "success", "data": refreshed_data}), 200
    except Exception as e:
        logging.error(f"Error refreshing assets: {e}")
        return jsonify({"status": "error", "message": f"Error in GET Method `/refresh_assets`: " + str(e)}), 500

# Ore Calculator endpoints
@app.route("/facilities", methods=["GET"])
def facilities():
    """
    Get all facilities.
    """
    try:
        facilities = get_all_facilities()
        return jsonify({"status": "success", "data": facilities}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error in GET Method `/facilities`: " + str(e)}), 500

@app.route("/optimize", methods=["POST"])
def optimize():
    """
    Body: {
      "demands": { "Tritanium": qty, ... },
      "character_id": int,
      "implant_pct": int,
      "facility_id": int,
      "ore_ids": [optional],
      "mode": "min_cost"
    }
    """
    try:
        payload = request.get_json(force=True) or {}
        demands = payload["demands"]
        character_id = payload["character_id"]
        implant_pct = payload.get("implant_pct", 0)
        facility_id = payload["facility_id"]
        mode = payload.get("mode", "min_cost")
        opt_only_compressed = payload.get("only_compressed", False)

        # 1. Get character skills
        character = char_manager.get_character_by_id(character_id)
        if not character:
            return jsonify({"status": "error", "message": f"Error in POST Method `/optimize`: Character ID {character_id} not found"}), 400
        
        char_adapter(character)
        skills = get_character_skills()
        implants = get_character_implants(implant_pct)
        
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

        # --- Reprocessing fee calculation ---
        reprocessing_tax_rate = facility.get("tax", 0.0)  # e.g., 0.01 for 1%
        yielded_value = 0.0
        for mat, qty in total_yielded_materials.items():
            price = None
            if mat in mat_name_to_price:
                price = mat_name_to_price[mat]
            else:
                price = req_mat_prices.get(mat, None)
            if price is not None:
                yielded_value += qty * price

        reprocessing_fee = yielded_value * reprocessing_tax_rate
        result["reprocessing_fee"] = reprocessing_fee
        result["total_cost_with_reprocessing"] = result.get("total_cost", 0.0) + reprocessing_fee

        return jsonify({"status": "success", "data": result}), 200
    except KeyError as ke:
        return jsonify({"status": "error", "message": f"Error in POST Method `/optimize`: Missing field {ke}"}), 400
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error in POST Method `/optimize`: " + str(e)}), 500
                       
@app.route("/materials", methods=["GET"])
def materials():
    global MATERIALS
    try:
        if MATERIALS is None:
            MATERIALS = get_all_materials()
        return jsonify({"status": "success", "materials": MATERIALS})
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error in GET Method `/materials`: " + str(e), "data": []}), 500

@app.route("/ores", methods=["GET"])
def ores():
    try:
        ores = get_all_ores()
        return ores
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error in GET Method `/ores`: " + str(e), "data": []}), 500

# Market Orders endpoints
@app.route("/refresh_market_orders", methods=["GET"])
def refresh_market_orders():
    try:
        refreshed_data = char_manager.refresh_market_orders()
        now = datetime.now(timezone.utc)
        station_cache = {}
        region_cache = {}
        refreshed_orders = []
        for character in refreshed_data:
            # Character name lookup
            if isinstance(character, str):
                character = json.loads(character)
            character_name = character.get("character_name")
            for order in character.get("market_orders", []):
                # Cache station lookup
                location_id = order['location_id']
                if location_id in station_cache:
                    station = station_cache[location_id]
                else:
                    station = get_location_info(location_id)
                    station_cache[location_id] = station

                # Cache region lookup
                region_id = order['region_id']
                if region_id in region_cache:
                    region = region_cache[region_id]
                else:
                    region = get_location_info(region_id)
                    region_cache[region_id] = region

                owner = ""
                if order['is_corporation']:
                    owner = corp_manager.get_corporation_name_by_character_id(order['character_id'])
                else:
                    owner = character_name
                
                # Calculate expires_in
                issued_dt = datetime.fromisoformat(order['issued'].replace("Z", "+00:00"))
                expires_dt = issued_dt + timedelta(days=order['duration'])
                expires_in = expires_dt - now
                # Format as days/hours
                if expires_in.total_seconds() > 0:
                    days = expires_in.days
                    hours = expires_in.seconds // 3600
                    mins = expires_in.seconds % 3600 // 60
                    expires_in = f"{days}d {hours}h {mins}m"
                else:
                    expires_in = "Expired"

                price_difference = 0
                price_status = "⚪N/A"
                if order['is_buy_order']:
                    order_book = get_type_buyprices([order['type_id']])
                    prices_list = []
                    if isinstance(order_book, dict):
                        prices_list = order_book.get(order['type_id'], [])
                    if prices_list and all(isinstance(o, dict) for o in prices_list):
                        highest_price = max(o["price"] for o in prices_list if "price" in o)
                        price_difference = order['price'] - highest_price
                        price_status = "🟢Best price" if price_difference > 0 else "🔴Undercut"
                else:
                    order_book = get_type_sellprices([order['type_id']])
                    prices_list = []
                    if isinstance(order_book, dict):
                        prices_list = order_book.get(order['type_id'], [])
                    if prices_list and all(isinstance(o, dict) for o in prices_list):
                        lowest_price = min(o["price"] for o in prices_list if "price" in o)
                        price_difference = order['price'] - lowest_price
                        price_status = "🔴Undercut" if price_difference > 0 else "🟢Best price"

                refreshed_orders.append({
                    "Owner": owner,
                    "Type ID": order['type_id'],
                    "Type": order['type_name'],
                    "Price": order['price'],
                    "Price Status": price_status,
                    "Price Difference": price_difference,
                    "Volume": str(order['volume_remain']) + '/' + str(order['volume_total']),
                    "Total Price": order['price'] * order['volume_remain'],
                    "Range": order['range'],
                    "Min. Volume": order['min_volume'],
                    "Expires In": expires_in,
                    "Escrow Remaining": order.get('escrow', 0),
                    "Station": station.get('station_name', location_id),
                    "Region": region.get('name', region_id),
                    "is_buy_order": order['is_buy_order'],
                    "type_group_id": order.get('type_group_id', -1),
                    "type_group_name": order.get('type_group_name', 'Unknown'),
                    "type_category_id": order.get('type_category_id', -1),
                    "type_category_name": order.get('type_category_name', 'Unknown'),
                    "is_blueprint_copy": False
                })

        return jsonify({"status": "success", "data": refreshed_orders}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error in GET Method `/refresh_market_orders`: " + str(e)}), 500

@app.route("/location/<int:location_id>", methods=["GET"])
def location(location_id):
    try:
        location_info = get_location_info(location_id)
        return jsonify({"status": "success", "data": location_info}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": f"Error in GET Method `/location/{location_id}`: " + str(e)}), 500

@app.route('/locations', methods=['POST'])
def locations():
    try:
        data = request.get_json()
        location_ids = data.get("location_ids", [])
        result = {}
        for locaction_id in location_ids:
            info = get_location_info(locaction_id)
            result[str(locaction_id)] = info
        return jsonify({"status": "success", "data": result}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": "Error in POST Method `/locations`: " + str(e)}), 500

if __name__ == "__main__":
    app.run(host="localhost", port=5000, debug=True)