import requests
import zipfile
import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime

CORDIS_URL = "https://cordis.europa.eu/data/cordis-h2020projects-csv.zip"
ZIP_FILENAME = "cordis_h2020_projects.zip"
EXTRACT_DIR = "cordis_extracted"

def download_cordis_data(zip_path: Path):
    logging.info("Downloading CORDIS dataset...")
    response = requests.get(CORDIS_URL, stream=True)
    response.raise_for_status()
    with open(zip_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    logging.info("CORDIS dataset downloaded successfully.")

def extract_zip(zip_path: Path, extract_dir: Path):
    logging.info("Extracting CORDIS dataset...")
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    logging.info("CORDIS dataset extracted successfully.")

def process_cordis_data(extract_dir: Path, output_dir: Path):
    logging.info("Processing CORDIS dataset...")

    projects_csv = extract_dir / "project.csv"
    org_csv = extract_dir / "organization.csv"

    projects_df = pd.read_csv(
        projects_csv, sep=';', encoding='utf-8', low_memory=False,
        on_bad_lines='skip', quoting=3, dtype=str
    )
    org_df = pd.read_csv(
        org_csv, sep=';', encoding='utf-8', low_memory=False,
        on_bad_lines='skip', quoting=3, dtype=str
    )

    # Remove quotes and strip spaces from column names
    projects_df.columns = projects_df.columns.str.replace('"', '').str.strip()
    org_df.columns = org_df.columns.str.replace('"', '').str.strip()

    # Verify actual columns again
    logging.info(f"Projects columns: {projects_df.columns.tolist()}")
    logging.info(f"Organization columns: {org_df.columns.tolist()}")

    # Continue with the previous logic as before
    projects_columns = ['id', 'acronym', 'title', 'startDate', 'endDate', 
                        'ecMaxContribution', 'totalCost', 'topics']

    missing_columns = [col for col in projects_columns if col not in projects_df.columns]
    if missing_columns:
        logging.error(f"Missing columns in projects CSV: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

    projects_nodes = projects_df[projects_columns].copy()
    projects_nodes.rename(columns={'id': 'projectId'}, inplace=True)
    projects_nodes['label'] = 'Project'

    org_columns = ['organisationID', 'name', 'shortName', 'country', 'vatNumber', 
                   'city', 'activityType', 'projectID', 'role', 'ecContribution']
    missing_org_columns = [col for col in org_columns if col not in org_df.columns]
    if missing_org_columns:
        logging.error(f"Missing columns in organizations CSV: {missing_org_columns}")
        raise ValueError(f"Missing columns: {missing_org_columns}")

    org_nodes = org_df[['organisationID', 'name', 'shortName', 'country', 'vatNumber', 
                        'city', 'activityType']].copy()
    org_nodes.rename(columns={'organisationID': 'organizationId'}, inplace=True)
    org_nodes['label'] = 'Organization'

    relationships_df = org_df[['projectID', 'organisationID', 'role', 'ecContribution']].copy()
    relationships_df.rename(columns={'projectID': 'projectId', 'organisationID': 'organizationId'}, inplace=True)
    relationships_df['type'] = 'PARTICIPATED_IN'

    # Output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save CSVs for Neo4j import
    projects_csv_out = output_dir / "projects_nodes.csv"
    organizations_csv_out = output_dir / "organizations_nodes.csv"
    relationships_csv_out = output_dir / "participated_in_relationships.csv"

    projects_nodes.to_csv(projects_csv_out, index=False)
    org_nodes.to_csv(organizations_csv_out, index=False)
    relationships_df.to_csv(relationships_csv_out, index=False)

    logging.info(f"Projects nodes saved: {projects_csv_out}")
    logging.info(f"Organizations nodes saved: {organizations_csv_out}")
    logging.info(f"Relationships saved: {relationships_csv_out}")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata for CORDIS dataset...")
    metadata = {
        "dataset": "CORDIS Horizon 2020 Projects Database",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": CORDIS_URL,
        "license": "European Union Open Data License",
        "description": "Graph-ready dataset of EU-funded R&D projects and organizations from Horizon 2020.",
        "files": {
            "projects_nodes": "projects_nodes.csv",
            "organizations_nodes": "organizations_nodes.csv",
            "relationships": "participated_in_relationships.csv"
        },
        "prepared_for": "Neo4j graph import",
    }

    metadata_path = output_dir / "metadata.json"
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=4)

    logging.info(f"Metadata saved: {metadata_path}")

def clean_up(zip_path: Path, extract_dir: Path):
    logging.info("Cleaning up temporary files...")
    if zip_path.exists():
        zip_path.unlink()
    if extract_dir.exists():
        for file in extract_dir.iterdir():
            file.unlink()
        extract_dir.rmdir()
    logging.info("Cleanup completed successfully.")

def retrieve_and_prepare_cordis(output_dir: Path):
    zip_path = output_dir / ZIP_FILENAME
    extract_dir = output_dir / EXTRACT_DIR

    extract_dir.mkdir(parents=True, exist_ok=True)

    try:
        download_cordis_data(zip_path=zip_path)
        extract_zip(zip_path=zip_path, extract_dir=extract_dir)
        process_cordis_data(extract_dir=extract_dir, output_dir=output_dir)
        generate_metadata(output_dir=output_dir)
    except Exception as e:
        logging.error(f"CORDIS retrieval failed: {e}")
        raise
    finally:
        clean_up(zip_path=zip_path, extract_dir=extract_dir)