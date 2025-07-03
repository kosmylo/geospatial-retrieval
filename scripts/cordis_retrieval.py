import requests
import zipfile
import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime
import csv

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

    files = {
        "projects": extract_dir / "project.csv",
        "organizations": extract_dir / "organization.csv",
        "topics": extract_dir / "topics.csv",
        "legalBasis": extract_dir / "legalBasis.csv",
    }

    # Load data robustly
    projects_df = pd.read_csv(files["projects"], sep=';', encoding='utf-8', dtype=str, engine='python', on_bad_lines='skip')
    org_df = pd.read_csv(files["organizations"], sep=';', encoding='utf-8', dtype=str, engine='python', on_bad_lines='skip')
    topics_df = pd.read_csv(files["topics"], sep=';', encoding='utf-8', dtype=str, engine='python', on_bad_lines='skip')
    legal_basis_df = pd.read_csv(files["legalBasis"], sep=';', encoding='utf-8', dtype=str, engine='python', on_bad_lines='skip')

    # Clean column names
    projects_df.columns = projects_df.columns.str.replace('"', '').str.strip()
    org_df.columns = org_df.columns.str.replace('"', '').str.strip()

    # Required columns
    required_proj_cols = ['id', 'acronym', 'title', 'objective', 'startDate', 'endDate',
                          'ecMaxContribution', 'totalCost', 'topics', 'legalBasis', 'frameworkProgramme']
    required_org_cols = ['organisationID', 'name', 'shortName', 'country', 'vatNumber', 
                         'city', 'activityType', 'projectID', 'role', 'ecContribution']

    project_nodes = projects_df[required_proj_cols].copy()
    project_nodes.rename(columns={'id': 'projectId'}, inplace=True)

    # Numeric cleaning
    for col in ['ecMaxContribution', 'totalCost']:
        project_nodes[col] = (
            project_nodes[col]
            .str.replace(r'[^\d,.]', '', regex=True)
            .str.replace(',', '.')
            .astype(float)
            .fillna(0)
        )

    # Date processing (ISO format)
    for date_col in ['startDate', 'endDate']:
        project_nodes[date_col] = pd.to_datetime(project_nodes[date_col], format='%Y-%m-%d', errors='coerce')
    project_nodes.dropna(subset=['startDate', 'endDate'], inplace=True)
    project_nodes['startDate'] = project_nodes['startDate'].dt.date
    project_nodes['endDate'] = project_nodes['endDate'].dt.date
    project_nodes['label'] = 'Project'

    # Organization Nodes
    org_nodes = org_df[required_org_cols[:-3]].copy()
    org_nodes.rename(columns={'organisationID': 'organizationId'}, inplace=True)
    org_nodes['label'] = 'Organization'

    # PARTICIPATED_IN Relationships
    relationships_participation = org_df[['projectID', 'organisationID', 'role', 'ecContribution']].copy()
    relationships_participation.rename(columns={'projectID': 'projectId', 'organisationID': 'organizationId'}, inplace=True)
    relationships_participation['ecContribution'] = relationships_participation['ecContribution'].str.replace(',', '.').astype(float).fillna(0)

    # Topics Nodes
    topic_nodes = topics_df[['topic', 'title']].drop_duplicates().copy()
    topic_nodes.rename(columns={'topic': 'topicId', 'title': 'topicTitle'}, inplace=True)
    topic_nodes['label'] = 'Topic'

    # HAS_TOPIC Relationships
    project_topics = topics_df[['projectID', 'topic']].copy()
    project_topics.rename(columns={'projectID': 'projectId', 'topic': 'topicId'}, inplace=True)

    # Legal Basis Nodes 
    legal_basis_nodes = legal_basis_df[['legalBasis', 'title']].drop_duplicates().copy()
    legal_basis_nodes.rename(columns={'legalBasis': 'legalBasisId', 'title': 'legalBasisTitle'}, inplace=True)
    legal_basis_nodes['label'] = 'LegalBasis'

    # HAS_LEGAL_BASIS Relationships
    project_legalbasis = legal_basis_df[['projectID', 'legalBasis']].copy()
    project_legalbasis.rename(columns={'projectID': 'projectId', 'legalBasis': 'legalBasisId'}, inplace=True)

    # Save CSV files for Neo4j import
    output_dir.mkdir(parents=True, exist_ok=True)
    project_nodes.to_csv(output_dir / "projects_nodes.csv", index=False)
    org_nodes.to_csv(output_dir / "organizations_nodes.csv", index=False)
    topic_nodes.to_csv(output_dir / "topics_nodes.csv", index=False)
    legal_basis_nodes.to_csv(output_dir / "legal_basis_nodes.csv", index=False)
    relationships_participation.to_csv(output_dir / "participated_in_relationships.csv", index=False)
    project_topics.to_csv(output_dir / "has_topic_relationships.csv", index=False)
    project_legalbasis.to_csv(output_dir / "has_legalbasis_relationships.csv", index=False)

    logging.info("CORDIS dataset processing completed successfully.")

def generate_metadata(output_dir: Path):
    logging.info("Generating metadata for CORDIS dataset...")
    metadata = {
        "dataset": "CORDIS Horizon 2020 Projects Database",
        "retrieval_date": datetime.utcnow().isoformat() + "Z",
        "source": CORDIS_URL,
        "license": "European Union Open Data License",
        "description": "Graph-ready dataset from Horizon 2020 CORDIS database with detailed project information and relationships.",
        "files": {
            "projects": "projects_nodes.csv",
            "organizations": "organizations_nodes.csv",
            "topics": "topics_nodes.csv",
            "legalBasis": "legal_basis_nodes.csv",
            "relationships": {
                "participated_in": "participated_in_relationships.csv",
                "has_topic": "has_topic_relationships.csv",
                "has_legal_basis": "has_legalbasis_relationships.csv"
            },
            "euroSciVoc": "euroSciVoc.csv",
            "webLink": "webLink.csv",
            "webItem": "webItem.csv"
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