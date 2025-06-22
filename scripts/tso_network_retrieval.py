import os
import requests
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path

API_TOKEN = os.getenv("ENTSOE_API_TOKEN")
BASE_URL = "https://web-api.tp.entsoe.eu/api"
OUTPUT_FILENAME = "entsoe_tso_network.csv"
METADATA_FILENAME = "entsoe_tso_network_metadata.json"

# Map of ENTSO-E countries and area codes (EIC codes already defined earlier)
TSO_CODES = {
    "Austria": "10YAT-APG------L",
    "Belgium": "10YBE----------2",
    "Bulgaria": "10YCA-BULGARIA-R",
    "Croatia": "10YHR-HEP------M",
    "Czech Republic": "10YCZ-CEPS-----N",
    "Denmark": "10Y1001A1001A65H",
    "Estonia": "10Y1001A1001A39I",
    "Finland": "10YFI-1--------U",
    "France": "10YFR-RTE------C",
    "Germany": "10Y1001A1001A83F",
    "Greece": "10YGR-HTSO-----Y",
    "Hungary": "10YHU-MAVIR----U",
    "Ireland": "10YIE-1001A00010",
    "Italy": "10YIT-GRTN-----B",
    "Latvia": "10YLV-1001A00074",
    "Lithuania": "10YLT-1001A0008Q",
    "Netherlands": "10YNL----------L",
    "Poland": "10YPL-AREA-----S",
    "Portugal": "10YPT-REN------W",
    "Romania": "10YRO-TEL------P",
    "Slovakia": "10YSK-SEPS-----K",
    "Slovenia": "10YSI-ELES-----O",
    "Spain": "10YES-REE------0",
    "Sweden": "10YSE-1--------K"
}

def fetch_tso_network_data():
    logging.info("Fetching ENTSO-E TSO Network data...")
    interconnections = []
    headers = {'Accept': 'application/json'}
    
    for country_from, area_from in TSO_CODES.items():
        for country_to, area_to in TSO_CODES.items():
            if area_from != area_to:
                params = {
                    'securityToken': API_TOKEN,
                    'documentType': 'A11',  # Physical Flow
                    'in_Domain': area_from,
                    'out_Domain': area_to,
                    'periodStart': datetime.utcnow().strftime('%Y%m%d0000'),
                    'periodEnd': datetime.utcnow().strftime('%Y%m%d2300')
                }
                response = requests.get(BASE_URL, params=params, headers=headers)
                if response.status_code == 200 and response.content:
                    if len(response.content) > 500:  # basic check to avoid empty responses
                        interconnections.append({
                            "tso_from": country_from,
                            "tso_to": country_to,
                            "from_area_code": area_from,
                            "to_area_code": area_to,
                            "status": "Connected"
                        })
                        logging.info(f"Connection found: {country_from} -> {country_to}")
                else:
                    logging.debug(f"No connection: {country_from} -> {country_to}")

    return interconnections

def save_interconnections(data, output_dir):
    df = pd.DataFrame(data)
    output_path = output_dir / OUTPUT_FILENAME
    df.to_csv(output_path, index=False)
    logging.info(f"ENTSO-E TSO Network data saved to {output_path}")

def generate_metadata(output_dir):
    metadata = {
        "dataset": "ENTSO-E TSO Network Interconnections",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": "ENTSO-E Transparency Platform API",
        "description": "Interconnections between TSOs in Europe, retrieved via ENTSO-E API, suitable for Neo4j graph import.",
        "columns": {
            "tso_from": "Originating TSO country",
            "tso_to": "Destination TSO country",
            "from_area_code": "ENTSO-E EIC area code of origin",
            "to_area_code": "ENTSO-E EIC area code of destination",
            "status": "Connection status (e.g., Connected)"
        },
        "prepared_for": "Neo4j graph import",
        "files": OUTPUT_FILENAME
    }

    metadata_path = output_dir / METADATA_FILENAME
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)
    logging.info(f"Metadata saved to {metadata_path}")

def retrieve_and_prepare_tso_network(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        data = fetch_tso_network_data()
        save_interconnections(data, output_dir)
        generate_metadata(output_dir)
    except Exception as e:
        logging.error(f"Error during TSO network retrieval: {e}")
        raise