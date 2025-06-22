import os
import logging
from pathlib import Path
from scripts.osm_retrieval import retrieve_osm_data

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

    logging.info(f"""
        RUN_OSM: {RUN_OSM}
    """)

    # Ensure output directory exists
    output_dir = Path("output/osm")
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- OpenStreetMap Geospatial Data Retrieval ---
    if RUN_OSM:
        try:
            retrieve_osm_data(output_dir=output_dir)
            logging.info("OSM data retrieved successfully.")
        except Exception as e:
            logging.error(f"OSM retrieval failed: {e}")

    logging.info("=== Geospatial Data Retrieval Completed ===")

if __name__ == "__main__":
    main()