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

def process_entsoe_tso_network(csv_path: Path, output_dir: Path):
    logging.info("Processing ENTSO-E TSO Network data...")

    # Explicitly load the CSV with UTF-8 encoding
    df = pd.read_csv(csv_path, encoding='utf-8')

    # Explicit Nodes Preparation
    tso_from_df = df[['tso_from', 'from_area_code']].rename(columns={'tso_from': 'country', 'from_area_code': 'area_code'})
    tso_to_df = df[['tso_to', 'to_area_code']].rename(columns={'tso_to': 'country', 'to_area_code': 'area_code'})
    tso_nodes_df = pd.concat([tso_from_df, tso_to_df]).drop_duplicates().reset_index(drop=True)

    # Explicit Relationships Preparation
    relationships_df = df.rename(columns={
        'tso_from': 'source_country',
        'tso_to': 'target_country',
        'status': 'status'
    })
    relationships_df = relationships_df[['source_country', 'target_country', 'status']]

    # Save explicitly prepared data
    output_dir.mkdir(parents=True, exist_ok=True)

    tso_nodes_csv = output_dir / "tso_nodes.csv"
    relationships_csv = output_dir / "interconnected_with_relationships.csv"

    tso_nodes_df.to_csv(tso_nodes_csv, index=False, encoding='utf-8')
    relationships_df.to_csv(relationships_csv, index=False, encoding='utf-8')

    logging.info(f"TSO nodes data saved to {tso_nodes_csv}")
    logging.info(f"Interconnection relationships data saved to {relationships_csv}")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata for ENTSO-E TSO Network...")
    metadata = {
        "dataset": "ENTSO-E TSO Network Interconnections",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": "ENTSO-E Transparency Platform API",
        "license": "ENTSO-E Data License",
        "description": "Nodes and relationships representing TSO interconnections in Europe, prepared explicitly for Neo4j import.",
        "nodes": {
            "TSO": {
                "country": "TSO country name",
                "area_code": "ENTSO-E area EIC code"
            }
        },
        "relationships": {
            "INTERCONNECTED_WITH": ["TSO", "TSO", {"status": "Connection status (e.g., Connected)"}]
        },
        "prepared_for": "Neo4j graph import",
        "files": {
            "nodes": "tso_nodes.csv",
            "relationships": "interconnected_with_relationships.csv"
        }
    }

    metadata_path = output_dir / "entsoe_metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)

    logging.info(f"Metadata saved to {metadata_path}")

def retrieve_and_prepare_tso_network(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        # Fetch raw TSO network data from ENTSO-E
        data = fetch_tso_network_data()
        
        # Save the raw interconnections data first (original format)
        save_interconnections(data, output_dir)
        
        # Now explicitly process the raw data into nodes and relationships for Neo4j
        raw_csv_path = output_dir / OUTPUT_FILENAME
        process_entsoe_tso_network(raw_csv_path, output_dir)
        
        # Generate enhanced metadata explicitly tailored for Neo4j
        generate_metadata(output_dir)
        
    except Exception as e:
        logging.error(f"Error during TSO network retrieval and preparation: {e}")
        raise