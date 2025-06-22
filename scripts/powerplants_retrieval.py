import os
import requests
import zipfile
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path

# Corrected URL
DATA_URL = "https://datasets.wri.org/private-admin/dataset/53623dfd-3df6-4f15-a091-67457cdb571f/resource/66bcdacc-3d0e-46ad-9271-a5a76b1853d2/download/globalpowerplantdatabasev130.zip"
ZIP_FILENAME = "global_power_plants.zip"
CSV_FILENAME = "global_power_plant_database.csv"
OUTPUT_FILENAME = "eu_powerplants.csv"
METADATA_FILENAME = "eu_powerplants_metadata.json"

EU_COUNTRIES_ISO = {
    "Austria": "AUT", "Belgium": "BEL", "Bulgaria": "BGR", "Croatia": "HRV",
    "Cyprus": "CYP", "Czech Republic": "CZE", "Denmark": "DNK", "Estonia": "EST",
    "Finland": "FIN", "France": "FRA", "Germany": "DEU", "Greece": "GRC",
    "Hungary": "HUN", "Ireland": "IRL", "Italy": "ITA", "Latvia": "LVA",
    "Lithuania": "LTU", "Luxembourg": "LUX", "Malta": "MLT", "Netherlands": "NLD",
    "Poland": "POL", "Portugal": "PRT", "Romania": "ROU", "Slovakia": "SVK",
    "Slovenia": "SVN", "Spain": "ESP", "Sweden": "SWE"
}

def download_powerplant_data(zip_path: Path):
    logging.info("Downloading Global Power Plant Database...")
    response = requests.get(DATA_URL, stream=True)
    response.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info("Download completed.")

def extract_zip(zip_path: Path, extract_dir: Path):
    logging.info("Extracting Global Power Plant Database...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    logging.info("Extraction completed.")

def process_powerplant_data(csv_path: Path, output_dir: Path):
    logging.info("Processing EU power plant data...")
    df = pd.read_csv(csv_path)

    # Filter for EU countries
    df_eu = df[df['country'].isin(EU_COUNTRIES_ISO.values())].copy()

    # Select and rename relevant columns
    df_eu = df_eu.rename(columns={
        "country": "country_iso",
        "name": "plant_name",
        "capacity_mw": "capacity_mw",
        "primary_fuel": "fuel_type",
        "latitude": "latitude",
        "longitude": "longitude",
        "owner": "owner",
        "commissioning_year": "commissioning_year",
        "source": "source"
    })

    df_eu = df_eu[[
        "plant_name", "country_iso", "capacity_mw", "fuel_type",
        "latitude", "longitude", "owner", "commissioning_year", "source"
    ]]

    # Save to CSV
    output_csv_path = output_dir / OUTPUT_FILENAME
    df_eu.to_csv(output_csv_path, index=False)
    logging.info(f"EU power plants data saved to {output_csv_path}")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata for EU power plants...")
    metadata = {
        "dataset": "Global Power Plant Database (EU Extract)",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": DATA_URL,
        "license": "CC BY 4.0",
        "description": "List of power plants in EU countries including owner, fuel type, capacity, and geographic location.",
        "columns": {
            "plant_name": "Name of the power plant",
            "country_iso": "ISO3 country code",
            "capacity_mw": "Installed capacity in megawatts",
            "fuel_type": "Primary fuel type",
            "latitude": "Latitude coordinate",
            "longitude": "Longitude coordinate",
            "owner": "Plant owner/operator",
            "commissioning_year": "Year plant was commissioned",
            "source": "Data source or reference"
        },
        "prepared_for": "Neo4j graph import",
        "files": OUTPUT_FILENAME
    }

    metadata_path = output_dir / METADATA_FILENAME
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)

    logging.info(f"Metadata saved to {metadata_path}")

def clean_up(zip_path: Path, extract_dir: Path):
    logging.info("Cleaning up temporary files...")
    if zip_path.exists():
        zip_path.unlink()
    if extract_dir.exists():
        for file in extract_dir.iterdir():
            file.unlink()
        extract_dir.rmdir()
    logging.info("Cleanup completed.")

def retrieve_and_prepare_powerplants(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / ZIP_FILENAME
    extract_dir = output_dir / "extracted"

    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        download_powerplant_data(zip_path=zip_path)
        extract_zip(zip_path=zip_path, extract_dir=extract_dir)
        csv_path = extract_dir / CSV_FILENAME
        process_powerplant_data(csv_path=csv_path, output_dir=output_dir)
        generate_metadata(output_dir=output_dir)
    except Exception as e:
        logging.error(f"Error during power plant data retrieval: {e}")
        raise
    finally:
        clean_up(zip_path=zip_path, extract_dir=extract_dir)