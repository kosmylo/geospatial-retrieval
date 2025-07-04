import os
import logging
from pathlib import Path
from scripts.osm_retrieval import retrieve_osm_data
from scripts.gridkit_retrieval import retrieve_and_prepare_gridkit
from scripts.powerplants_retrieval import retrieve_and_prepare_powerplants
from scripts.tso_network_retrieval import retrieve_and_prepare_tso_network
from scripts.cordis_retrieval import retrieve_and_prepare_cordis
from scripts.osm_prepare_neo4j import prepare_osm_data_for_neo4j

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
    RUN_CORDIS = os.getenv("RUN_CORDIS", "1") == "1"

    logging.info(f"""
        RUN_OSM: {RUN_OSM},
        RUN_GRIDKIT: {RUN_GRIDKIT},
        RUN_POWERPLANTS: {RUN_POWERPLANTS},
        RUN_TSO_NETWORK: {RUN_TSO_NETWORK},
        RUN_CORDIS: {RUN_CORDIS}
    """)

    # Ensure output directories exist
    osm_geojson_dir = Path("output/osm/geojson")
    osm_neo4j_output_dir = Path("output/osm/neo4j_import")
    gridkit_output_dir = Path("output/gridkit")
    powerplants_output_dir = Path("output/powerplants")
    tso_output_dir = Path("output/tso_network")
    cordis_output_dir = Path("output/cordis")

    osm_geojson_dir.mkdir(parents=True, exist_ok=True)
    osm_neo4j_output_dir.mkdir(parents=True, exist_ok=True)
    gridkit_output_dir.mkdir(parents=True, exist_ok=True)
    powerplants_output_dir.mkdir(parents=True, exist_ok=True)
    tso_output_dir.mkdir(parents=True, exist_ok=True)
    cordis_output_dir.mkdir(parents=True, exist_ok=True)

    # --- OpenStreetMap Geospatial Data Retrieval ---
    if RUN_OSM:
        try:
            retrieve_osm_data(output_dir=osm_geojson_dir)
            logging.info("OSM data retrieved successfully.")

            countries = ["Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic", "Denmark", 
                        "Estonia", "Finland", "France", "Germany", "Greece", "Hungary", "Ireland", "Italy", 
                        "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands", "Poland", "Portugal", 
                        "Romania", "Slovakia", "Slovenia", "Spain", "Sweden"
                        ]
            
            # Explicitly convert GeoJSON files to CSV for Neo4j:
            logging.info("Preparing OSM data for Neo4j import for all countries.")
            prepare_osm_data_for_neo4j(osm_geojson_dir, osm_neo4j_output_dir, countries)
            logging.info("OSM data explicitly prepared for Neo4j import.")
        except Exception as e:
            logging.error(f"OSM retrieval or preparation failed: {e}")
    
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
    
    # --- CORDIS Projects Data Retrieval ---
    if RUN_CORDIS:
        try:
            retrieve_and_prepare_cordis(output_dir=cordis_output_dir)
            logging.info("CORDIS data retrieved successfully.")
        except Exception as e:
            logging.error(f"CORDIS retrieval failed: {e}")

    logging.info("=== Geospatial Data Retrieval Completed ===")

if __name__ == "__main__":
    main()