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
    df = pd.read_csv(csv_path, encoding='utf-8')

    # Filter for EU countries explicitly
    df_eu = df[df['country'].isin(EU_COUNTRIES_ISO.values())].copy()

    # Rename columns explicitly
    df_eu.rename(columns={
        "country": "country_iso",
        "name": "plant_name",
        "capacity_mw": "capacity_mw",
        "primary_fuel": "fuel_type",
        "latitude": "latitude",
        "longitude": "longitude",
        "owner": "owner",
        "commissioning_year": "commissioning_year",
        "source": "source"
    }, inplace=True)

    # Select relevant columns explicitly
    df_eu = df_eu[[
        "plant_name", "country_iso", "capacity_mw", "fuel_type",
        "latitude", "longitude", "owner", "commissioning_year", "source"
    ]]

    # Create nodes explicitly
    powerplants_nodes = df_eu[["plant_name", "capacity_mw", "latitude", "longitude", "commissioning_year", "source"]].copy()
    powerplants_nodes.drop_duplicates(subset=['plant_name'], inplace=True)

    countries_nodes = df_eu[["country_iso"]].drop_duplicates().copy()
    owners_nodes = df_eu[["owner"]].dropna().drop_duplicates().copy()
    owners_nodes.rename(columns={'owner': 'name'}, inplace=True)

    fuel_types_nodes = df_eu[["fuel_type"]].drop_duplicates().copy()
    fuel_types_nodes.rename(columns={'fuel_type': 'type'}, inplace=True)

    # Relationships explicitly
    located_in_relationships = df_eu[["plant_name", "country_iso"]].dropna().copy()
    owned_by_relationships = df_eu[["plant_name", "owner"]].dropna().copy()
    uses_fuel_relationships = df_eu[["plant_name", "fuel_type"]].dropna().copy()

    # Prepare output directory explicitly
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save Nodes explicitly
    powerplants_nodes.to_csv(output_dir / "powerplants_nodes.csv", index=False, encoding='utf-8')
    countries_nodes.to_csv(output_dir / "countries_nodes.csv", index=False, encoding='utf-8')
    owners_nodes.to_csv(output_dir / "owners_nodes.csv", index=False, encoding='utf-8')
    fuel_types_nodes.to_csv(output_dir / "fuel_types_nodes.csv", index=False, encoding='utf-8')

    # Save Relationships explicitly
    located_in_relationships.to_csv(output_dir / "located_in_relationships.csv", index=False, encoding='utf-8')
    owned_by_relationships.to_csv(output_dir / "owned_by_relationships.csv", index=False, encoding='utf-8')
    uses_fuel_relationships.to_csv(output_dir / "uses_fuel_relationships.csv", index=False, encoding='utf-8')

    logging.info("All node and relationship CSV files generated successfully.")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata for EU power plants...")
    metadata = {
        "dataset": "Global Power Plant Database (EU Extract)",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": "https://datasets.wri.org/dataset/globalpowerplantdatabase",  # replace DATA_URL if defined elsewhere
        "license": "CC BY 4.0",
        "description": "Nodes and relationships for power plants in EU countries for Neo4j graph import.",
        "nodes": {
            "PowerPlant": {
                "plant_name": "Name of the power plant",
                "capacity_mw": "Installed capacity in megawatts",
                "latitude": "Latitude coordinate",
                "longitude": "Longitude coordinate",
                "commissioning_year": "Year the plant was commissioned",
                "source": "Data source or reference"
            },
            "Country": {
                "country_iso": "ISO3 country code"
            },
            "Owner": {
                "name": "Owner/operator name"
            },
            "FuelType": {
                "type": "Type of primary fuel"
            }
        },
        "relationships": {
            "LOCATED_IN": ["PowerPlant", "Country"],
            "OWNED_BY": ["PowerPlant", "Owner"],
            "USES_FUEL": ["PowerPlant", "FuelType"]
        },
        "prepared_for": "Neo4j graph import",
        "files": {
            "nodes": [
                "powerplants_nodes.csv",
                "countries_nodes.csv",
                "owners_nodes.csv",
                "fuel_types_nodes.csv"
            ],
            "relationships": [
                "located_in_relationships.csv",
                "owned_by_relationships.csv",
                "uses_fuel_relationships.csv"
            ]
        }
    }

    metadata_path = output_dir / "eu_powerplants.json"
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