import requests
import zipfile
import pandas as pd
import json
import logging
from datetime import datetime
from pathlib import Path

DATA_URL = "https://zenodo.org/records/47317/files/gridkit_euorpe.zip?download=1"
ZIP_FILENAME = "gridkit_data.zip"
EXTRACT_DIR = "gridkit_extracted"

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

    # Load CSVs
    nodes_df = pd.read_csv(vertices_path)
    links_df = pd.read_csv(edges_path)

    # Check the available columns for nodes_df
    logging.info(f"Nodes columns: {nodes_df.columns.tolist()}")

    # Cleaning nodes (removed 'country' column)
    nodes_df.rename(columns={'v_id': 'id', 'lon': 'longitude', 'lat': 'latitude'}, inplace=True)
    nodes_df = nodes_df[['id', 'longitude', 'latitude', 'typ', 'frequency', 'voltage', 'operator', 'name']]
    nodes_df['label'] = 'Substation'

    # Prepare relationships
    relationships_df = links_df.rename(columns={
        'v_id_1': 'source', 
        'v_id_2': 'target', 
        'cables': 'cables', 
        'voltage': 'voltage', 
        'wires': 'wires'
    })
    relationships_df = relationships_df[['source', 'target', 'cables', 'voltage', 'wires']]
    relationships_df['type'] = 'CONNECTED_TO'

    # Save nodes and relationships
    nodes_csv = output_dir / "nodes.csv"
    relationships_csv = output_dir / "relationships.csv"

    nodes_df.to_csv(nodes_csv, index=False)
    relationships_df.to_csv(relationships_csv, index=False)

    logging.info(f"Nodes data saved to {nodes_csv}")
    logging.info(f"Relationships data saved to {relationships_csv}")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata...")
    metadata = {
        "dataset": "GridKit European Transmission Grid",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": DATA_URL,
        "license": "ODbL",
        "description": "European high-voltage electricity transmission grid data including substations (nodes) and transmission lines (edges).",
        "files": {
            "nodes": "nodes.csv",
            "relationships": "relationships.csv"
        },
        "prepared_for": "Neo4j graph import",
    }

    metadata_path = output_dir / "metadata.json"
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