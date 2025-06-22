# Geospatial-Retrieval

A Docker Compose–based pipeline designed to **retrieve, process, and store detailed geospatial infrastructure data** from OpenStreetMap (OSM), specifically focused on energy-related facilities and infrastructures across EU countries. This repository is ideal for creating comprehensive spatial datasets to support multimodal AI applications and geospatial analytics in the energy domain.

## 🚀 Features

- **OpenStreetMap Geospatial Data Retrieval**:
  - Locations and attributes of power generation facilities (e.g., power plants, wind turbines, solar farms).
  - High-voltage transmission infrastructure, including substations and power lines.
  - EV charging stations and other energy-related points of interest.

- **Structured GeoJSON Output**:
  - Spatial data saved as standardized GeoJSON files.
  - Automatically generated detailed JSON metadata for each dataset, including source, retrieval timestamps, feature counts, and licensing information.

- **Flexible Configuration**:
  - Toggle data retrieval through environment flags (`RUN_OSM`).
  - Easily extend or modify queries for specific infrastructure categories directly in the retrieval scripts.

- **Robust Logging**:
  - Comprehensive logs saved in `logs/geospatial_retrieval.log`.
  - Real-time feedback on data retrieval progress, successes, and failures.

## 🗂 Repository Structure

```text
geospatial_retrieval
├── .env
├── .gitignore
├── Dockerfile
├── README.md
├── docker-compose.yaml
├── logs
│   └── geospatial_retrieval.log
├── main.py
├── output
│   └── osm
│       ├── germany_power_plants.geojson
│       ├── germany_power_plants_metadata.json
│       └── ...
├── requirements.txt
└── scripts
    └── osm_retrieval.py
```

- `main.py`: Coordinates geospatial data retrieval based on environment configurations.

- `scripts/osm_retrieval.py`: Performs OSM Overpass API queries and processes data into GeoJSON files.

- `docker-compose.yaml`: Defines Docker Compose service configurations.

- `Dockerfile`: Sets up Docker container and Python dependencies.

## 🔧 Prerequisites

- Docker & Docker Compose installed locally.

## ⚙️ Configuration

Configure the image retrieval pipeline by creating and editing a `.env` file at the repository root:

```bash
RUN_OSM=1
```

Create your `.env` file in the repository root with:

```bash
touch .env
```

Open the file with a text editor to add your values, for example:

```bash
nano .env
```

Set your preferred retrieval parameters and toggles accordingly.

### OSM Retrieval Categories

Pre-defined queries in `scripts/osm_retrieval.py` include:

- **Power Plants**
- **Wind Turbines**
- **Solar Farms**
- **Substations**
- **Transmission Lines**
- **EV Charging Stations**

You can adjust or extend these queries directly within the script as needed.

## 📂 Output

Datasets and metadata structured as follows:

```text
output/osm/
├── germany_power_plants.geojson
├── germany_power_plants_metadata.json
├── germany_wind_turbines.geojson
├── germany_wind_turbines_metadata.json
└── ...
```

Each GeoJSON file follows this standard structure:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [longitude, latitude]
      },
      "properties": {
        "osm_id": "123456789",
        "name": "Facility Name",
        "source": "wind",
        "capacity": "5000 kW",
        "...": "..."
      }
    },
    "..."
  ]
}
```

Metadata files provide additional context for each dataset:

```json
{
  "country": "Germany",
  "dataset": "wind_turbines",
  "number_of_features": 1234,
  "retrieval_timestamp": "2024-06-22T00:00:00Z",
  "source": "OpenStreetMap via Overpass API",
  "license": "ODbL (Open Database License)",
  "osm_query": "node[\"power\"=\"generator\"][\"generator:source\"=\"wind\"];",
  "geojson_file": "output/osm/germany_wind_turbines.geojson"
}
```

## 🐳 Build & Run

1. **Build the image**  from the repository root:

   ```bash
    docker build -t images-collector .
   ```

2. **Run the service** using Docker Compose:

   ```bash
   docker-compose up
   ```

   All scraper settings can be customized via environment variables in `.env`.

3. **Stop the service** when finished:

   ```bash
   docker-compose down
   ```

Retrieved datasets and metadata will be stored in the `output/osm/` directory, ready for further analysis and integration into your AI and geospatial pipelines.