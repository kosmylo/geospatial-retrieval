import requests
import zipfile
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path
import reverse_geocoder as rg

DATA_URL = "https://zenodo.org/records/47317/files/gridkit_euorpe.zip?download=1"
ZIP_FILENAME = "gridkit_data.zip"
EXTRACT_DIR = "gridkit_extracted"

def assign_label(typ):
    typ_lower = str(typ).strip().lower()
    if typ_lower in ['substation', 'sub_station', 'station']:
        return 'Substation'
    elif typ_lower == 'plant':
        return 'Plant'
    elif typ_lower == 'joint':
        return 'Joint'
    elif typ_lower == 'merge':
        return 'Merge'
    else:
        return 'Unknown'
    
def clean_name(name):
    if pd.isnull(name) or not isinstance(name, str):
        return 'Unknown'
    try:
        return name.encode('utf-8').decode('utf-8')
    except UnicodeDecodeError:
        return name.encode('utf-8', errors='replace').decode('utf-8')
    
def get_country(lat, lon):
    coordinates = (lat, lon)
    result = rg.search(coordinates, mode=1)  # Fast single query mode
    return result[0]['cc']  # ISO 2-letter country code

def assign_country(row):
    try:
        return get_country(float(row['latitude']), float(row['longitude']))
    except Exception as e:
        logging.warning(f"Could not get country for ID {row['id']}: {e}")
        return 'Unknown'

def download_gridkit_data(zip_path: Path):
    logging.info("Downloading GridKit dataset...")
    response = requests.get(DATA_URL, stream=True)
    response.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info("Download completed.")

def extract_zip(zip_path: Path, extract_dir: Path):
    logging.info("Extracting GridKit data...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    logging.info("Extraction completed.")

def process_gridkit_data(extract_dir: Path, output_dir: Path):
    logging.info("Processing GridKit data...")

    vertices_path = extract_dir / "gridkit_europe-highvoltage-vertices.csv"
    edges_path = extract_dir / "gridkit_europe-highvoltage-links.csv"

    # Load CSVs explicitly with UTF-8 encoding
    nodes_df = pd.read_csv(vertices_path, encoding='utf-8', dtype=str)
    links_df = pd.read_csv(edges_path, encoding='utf-8', dtype=str)

    # Rename and select columns explicitly
    nodes_df.rename(columns={
        'v_id': 'id', 'lon': 'longitude', 'lat': 'latitude',
        'typ': 'type', 'frequency': 'frequency', 'voltage': 'voltage',
        'operator': 'operator', 'name': 'name'
    }, inplace=True)

    nodes_df = nodes_df[['id', 'longitude', 'latitude', 'type', 'frequency', 'voltage', 'operator', 'name']]
    nodes_df['label'] = nodes_df['type'].apply(assign_label)
    nodes_df['name'] = nodes_df['name'].apply(clean_name)
    nodes_df['country'] = nodes_df.apply(assign_country, axis=1)

    # Relationships explicitly
    relationships_df = links_df.rename(columns={
        'v_id_1': 'source', 
        'v_id_2': 'target', 
        'cables': 'cables', 
        'voltage': 'voltage', 
        'wires': 'wires'
    })
    relationships_df = relationships_df[['source', 'target', 'cables', 'voltage', 'wires']]
    relationships_df['type'] = 'CONNECTED_TO'

    # Save explicitly for Neo4j
    output_dir.mkdir(parents=True, exist_ok=True)

    nodes_csv = output_dir / "grid_nodes.csv"
    relationships_csv = output_dir / "connected_to_relationships.csv"

    nodes_df.to_csv(nodes_csv, index=False, encoding='utf-8')
    relationships_df.to_csv(relationships_csv, index=False, encoding='utf-8')

    logging.info(f"Nodes data saved to {nodes_csv}")
    logging.info(f"Relationships data saved to {relationships_csv}")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata...")
    metadata = {
        "dataset": "GridKit European Transmission Grid",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": DATA_URL,
        "license": "ODbL",
        "description": "European high-voltage electricity transmission grid data including buses (nodes) and transmission lines (edges).",
        "files": {
            "nodes": "grid_nodes.csv",
            "relationships": "connected_to_relationships.csv"
        },
        "prepared_for": "Neo4j graph import",
    }

    metadata_path = output_dir / "gridkit.json"
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

def retrieve_and_prepare_gridkit(output_dir: Path):
    zip_path = output_dir / ZIP_FILENAME
    extract_dir = output_dir / EXTRACT_DIR

    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        download_gridkit_data(zip_path=zip_path)
        extract_zip(zip_path=zip_path, extract_dir=extract_dir)
        process_gridkit_data(extract_dir=extract_dir, output_dir=output_dir)
        generate_metadata(output_dir=output_dir)
    except Exception as e:
        logging.error(f"Error during GridKit retrieval: {e}")
        raise
    finally:
        clean_up(zip_path=zip_path, extract_dir=extract_dir)