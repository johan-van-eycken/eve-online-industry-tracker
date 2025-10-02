from flask import Flask, request, jsonify
import logging
import os
import sys

# Add project root to sys.path
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(ROOT_DIR)

from classes.config_manager import ConfigManager
from classes.database_manager import DatabaseManager
from classes.character_manager import CharacterManager
from config.schemas import CONFIG_SCHEMA

app = Flask(__name__)

# Initialize configuration and managers
logging.basicConfig(level=logging.INFO)

try:
    cfgManager = ConfigManager(base_path="config/config.json", schema=CONFIG_SCHEMA, secret_path="config/secret.json")
    cfg = cfgManager.all()

    db_oauth = DatabaseManager(cfg["app"]["database_oauth_uri"])
    db_app = DatabaseManager(cfg["app"]["database_app_uri"])
    db_sde = DatabaseManager(cfg["app"]["database_sde_uri"])

    char_manager = CharacterManager(cfgManager, db_oauth, db_app, db_sde, char_manager_all=cfg["characters"])
except Exception as e:
    logging.error(f"Failed to initialize Flask app: {e}")
    raise e

@app.route('/refresh_wallet_balances', methods=['POST'])
def refresh_wallet_balances():
    """
    Endpoint to refresh wallet balances for characters.
    Accepts an optional 'character_name' parameter to refresh a specific character.
    """
    try:
        refreshed_data = char_manager.refresh_wallet_balance()
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

@app.route("/shutdown", methods=["POST"])
def shutdown():
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()
    return 'Server shutting down...'

if __name__ == '__main__':
    app.run(host="localhost", port=5000, debug=True)