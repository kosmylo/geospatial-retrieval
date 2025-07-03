import pandas as pd
import json
import logging
from pathlib import Path

def geojson_to_csv(geojson_path, country_name, output_dir, node_type):
    with open(geojson_path, 'r', encoding='utf-8') as f:
        geojson = json.load(f)

    rows = []
    for feature in geojson["features"]:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]
        row = {
            "osm_id": props.get("osm_id"),
            "latitude": coords[1],
            "longitude": coords[0],
            "country": country_name
        }

        # Additional properties based on node_type
        if node_type == "ChargingStation":
            row.update({
                "name": props.get("name"),
                "operator": props.get("operator"),
                "capacity": props.get("capacity"),
                "opening_hours": props.get("opening_hours"),
                "phone": props.get("phone"),
                "website": props.get("website"),
                "socket_types": ";".join([k for k in props.keys() if k.startswith("socket:")])
            })
        elif node_type == "PowerPlant":
            row.update({
                "name": props.get("name"),
                "operator": props.get("operator"),
                "source": props.get("plant:source"),
                "method": props.get("plant:method"),
                "capacity": props.get("plant:output:electricity")
            })
        elif node_type == "SolarFarm":
            row.update({
                "operator": props.get("operator"),
                "source": props.get("generator:source"),
                "method": props.get("generator:method"),
                "capacity": props.get("generator:output:electricity")
            })
        elif node_type == "WindTurbine":
            row.update({
                "operator": props.get("operator"),
                "manufacturer": props.get("manufacturer"),
                "model": props.get("model"),
                "source": props.get("generator:source"),
                "method": props.get("generator:method"),
                "capacity": props.get("generator:output:electricity"),
                "rotor_diameter": props.get("rotor:diameter")
            })
        elif node_type == "Substation":
            pass
        elif node_type == "TransmissionLine":
            row.update({
                "name": props.get("name"),
                "operator": props.get("operator"),
                "voltage": props.get("voltage"),
                "circuits": props.get("circuits"),
                "cables": props.get("cables"),
                "wires": props.get("wires"),
                "start_date": props.get("start_date")
            })

        rows.append(row)

    df = pd.DataFrame(rows)

    # Save nodes CSV
    node_csv_path = output_dir / f"{node_type.lower()}s_nodes.csv"
    df.to_csv(node_csv_path, index=False, encoding='utf-8')
    logging.info(f"Nodes CSV saved: {node_csv_path}")

    # Save relationships CSV (to Country)
    relationships_df = df[["osm_id", "country"]].rename(columns={
        "osm_id": "source_id", 
        "country": "target_country"
    })
    rel_csv_path = output_dir / f"{node_type.lower()}s_located_in_relationships.csv"
    relationships_df.to_csv(rel_csv_path, index=False, encoding='utf-8')
    logging.info(f"Relationships CSV saved: {rel_csv_path}")

def prepare_osm_data_for_neo4j(geojson_dir: Path, output_dir: Path, country_name: str):
    output_dir.mkdir(parents=True, exist_ok=True)

    dataset_mapping = {
        "ev_charging_stations": "ChargingStation",
        "power_plants": "PowerPlant",
        "solar_farms": "SolarFarm",
        "wind_turbines": "WindTurbine",
        "substations": "Substation",
        "transmission_lines": "TransmissionLine"
    }

    for dataset, node_type in dataset_mapping.items():
        geojson_filename = f"{country_name.lower()}_{dataset}.geojson"
        geojson_path = geojson_dir / geojson_filename

        if geojson_path.exists():
            logging.info(f"Processing {geojson_filename}...")
            geojson_to_csv(geojson_path, country_name, output_dir, node_type)
        else:
            logging.warning(f"File not found: {geojson_path}")

def generate_country_nodes(output_dir: Path, countries: list):
    """Generate explicit country nodes CSV."""
    rows = [{"country_name": country} for country in countries]
    df = pd.DataFrame(rows)
    country_csv_path = output_dir / "countries_nodes.csv"
    df.to_csv(country_csv_path, index=False, encoding='utf-8')
    logging.info(f"Country nodes CSV saved: {country_csv_path}")