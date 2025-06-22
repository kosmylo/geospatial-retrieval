import os
import logging
from pathlib import Path
from scripts.osm_retrieval import retrieve_osm_data
from scripts.gridkit_retrieval import retrieve_and_prepare_gridkit
from scripts.powerplants_retrieval import retrieve_and_prepare_powerplants
from scripts.tso_network_retrieval import retrieve_and_prepare_tso_network

def configure_logging():
    """Configure logging for the application."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "geospatial_retrieval.log"),
            logging.StreamHandler()
        ]
    )

def main():
    """Main function to orchestrate geospatial data retrieval."""
    configure_logging()
    logging.info("=== Starting Geospatial Data Retrieval ===")

    # Environment-driven toggles
    RUN_OSM = os.getenv("RUN_OSM", "1") == "1"
    RUN_GRIDKIT = os.getenv("RUN_GRIDKIT", "1") == "1"
    RUN_POWERPLANTS = os.getenv("RUN_POWERPLANTS", "1") == "1"
    RUN_TSO_NETWORK = os.getenv("RUN_TSO_NETWORK", "1") == "1"

    logging.info(f"""
        RUN_OSM: {RUN_OSM},
        RUN_GRIDKIT: {RUN_GRIDKIT},
        RUN_POWERPLANTS: {RUN_POWERPLANTS},
        RUN_TSO_NETWORK: {RUN_TSO_NETWORK}
    """)

    # Ensure output directories exist
    osm_output_dir = Path("output/osm")
    gridkit_output_dir = Path("output/gridkit")
    powerplants_output_dir = Path("output/powerplants")
    tso_output_dir = Path("output/tso_network")

    osm_output_dir.mkdir(parents=True, exist_ok=True)
    gridkit_output_dir.mkdir(parents=True, exist_ok=True)
    powerplants_output_dir.mkdir(parents=True, exist_ok=True)
    tso_output_dir.mkdir(parents=True, exist_ok=True)

    # --- OpenStreetMap Geospatial Data Retrieval ---
    if RUN_OSM:
        try:
            retrieve_osm_data(output_dir=osm_output_dir)
            logging.info("OSM data retrieved successfully.")
        except Exception as e:
            logging.error(f"OSM retrieval failed: {e}")
    
    # --- GridKit European Transmission Grid Retrieval ---
    if RUN_GRIDKIT:
        try:
            retrieve_and_prepare_gridkit(output_dir=gridkit_output_dir)
            logging.info("GridKit data retrieved successfully.")
        except Exception as e:
            logging.error(f"GridKit retrieval failed: {e}")

    # --- EU Power Plants & Ownership Retrieval (Global Power Plant Database) ---
    if RUN_POWERPLANTS:
        try:
            retrieve_and_prepare_powerplants(output_dir=powerplants_output_dir)
            logging.info("Power plants data retrieved successfully.")
        except Exception as e:
            logging.error(f"Power plants retrieval failed: {e}")

    # --- ENTSO-E TSO Network Retrieval ---
    if RUN_TSO_NETWORK:
        try:
            retrieve_and_prepare_tso_network(output_dir=tso_output_dir)
            logging.info("ENTSO-E TSO network data retrieved successfully.")
        except Exception as e:
            logging.error(f"ENTSO-E TSO network retrieval failed: {e}")

    logging.info("=== Geospatial Data Retrieval Completed ===")

if __name__ == "__main__":
    main()