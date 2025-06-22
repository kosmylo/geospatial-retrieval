import requests
import json
import os
import logging
from datetime import datetime

# OSM Overpass API URL
OVERPASS_URL = "http://overpass-api.de/api/interpreter"

# Define EU countries with ISO codes
EU_COUNTRIES = {
    "Austria": "AT", "Belgium": "BE", "Bulgaria": "BG", "Croatia": "HR", "Cyprus": "CY",
    "Czech Republic": "CZ", "Denmark": "DK", "Estonia": "EE", "Finland": "FI", "France": "FR",
    "Germany": "DE", "Greece": "GR", "Hungary": "HU", "Ireland": "IE", "Italy": "IT",
    "Latvia": "LV", "Lithuania": "LT", "Luxembourg": "LU", "Malta": "MT", "Netherlands": "NL",
    "Poland": "PL", "Portugal": "PT", "Romania": "RO", "Slovakia": "SK", "Slovenia": "SI",
    "Spain": "ES", "Sweden": "SE"
}

# Energy-related OSM queries without area specification (will be added dynamically)
ENERGY_QUERIES = {
    "power_plants": '["power"="plant"]',
    "wind_turbines": '["power"="generator"]["generator:source"="wind"]',
    "solar_farms": '["power"="generator"]["generator:source"="solar"]',
    "substations": '["power"="substation"]',
    "transmission_lines": '["power"="line"]',
    "ev_charging_stations": '["amenity"="charging_station"]'
}

def build_overpass_query(country_iso, osm_filter):
    """Build the correct Overpass query using area filter."""
    return f"""
    [out:json][timeout:300];
    area["ISO3166-1"="{country_iso}"][admin_level=2]->.searchArea;
    (
      node(area.searchArea){osm_filter};
      way(area.searchArea){osm_filter};
      relation(area.searchArea){osm_filter};
    );
    out center;
    """

def fetch_osm_data(country, country_iso, dataset_name, osm_filter):
    """Fetches data from OSM Overpass API."""
    logging.info(f"Fetching {dataset_name} for {country}")
    query = build_overpass_query(country_iso, osm_filter)
    response = requests.post(OVERPASS_URL, data={'data': query})

    if response.status_code == 200:
        data = response.json()
        logging.info(f"Retrieved {len(data.get('elements', []))} elements for {dataset_name} in {country}")
        return data
    else:
        logging.error(f"Failed retrieval for {dataset_name} in {country}: {response.status_code} - {response.text}")
        return None

def save_geojson(output_dir, data, country, dataset_name):
    """Saves data as GeoJSON and metadata."""
    features = []
    for elem in data.get('elements', []):
        # Determine geometry type
        if 'lon' in elem and 'lat' in elem:
            geometry = {"type": "Point", "coordinates": [elem['lon'], elem['lat']]}
        elif 'center' in elem:
            geometry = {"type": "Point", "coordinates": [elem['center']['lon'], elem['center']['lat']]}
        else:
            continue  # Skip if no geometry available

        properties = elem.get('tags', {})
        properties["osm_id"] = elem["id"]

        feature = {
            "type": "Feature",
            "geometry": geometry,
            "properties": properties
        }
        features.append(feature)

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    geojson_path = os.path.join(output_dir, f"{country.lower().replace(' ', '_')}_{dataset_name}.geojson")
    with open(geojson_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False, indent=4)

    metadata = {
        "country": country,
        "dataset": dataset_name,
        "number_of_features": len(features),
        "retrieval_timestamp": datetime.utcnow().isoformat() + "Z",
        "source": "OpenStreetMap via Overpass API",
        "license": "ODbL (Open Database License)",
        "osm_query": build_overpass_query(EU_COUNTRIES[country], ENERGY_QUERIES[dataset_name]).strip(),
        "geojson_file": geojson_path
    }

    metadata_path = geojson_path.replace('.geojson', '_metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as mf:
        json.dump(metadata, mf, ensure_ascii=False, indent=4)

    logging.info(f"Saved GeoJSON and metadata for {country}, dataset: {dataset_name}")

def retrieve_osm_data(output_dir="output/osm"):
    """Main function to retrieve and store OSM data."""
    os.makedirs(output_dir, exist_ok=True)
    for country, iso in EU_COUNTRIES.items():
        for dataset_name, osm_filter in ENERGY_QUERIES.items():
            try:
                data = fetch_osm_data(country, iso, dataset_name, osm_filter)
                if data:
                    save_geojson(output_dir, data, country, dataset_name)
            except Exception as e:
                logging.error(f"Exception for {country}, {dataset_name}: {e}")