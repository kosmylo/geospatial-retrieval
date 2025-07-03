# Geospatial-Retrieval

A Docker Compose–based pipeline designed to **retrieve, process, and store detailed geospatial and relational infrastructure data** including OpenStreetMap (OSM), GridKit, Global Power Plant Database, ENTSO-E TSO network, and CORDIS EU Projects. This repository is ideal for creating comprehensive spatial and relational datasets to support multimodal AI applications, geospatial analytics, and knowledge graph construction in the energy domain.

## 🚀 Features

### 🌍 **Geospatial Infrastructure Data**

- **OpenStreetMap (OSM)**:
  - Power plants, wind turbines, solar farms
  - Substations, transmission lines
  - EV charging stations

- **GridKit European Transmission Grid**:
  - High-voltage grid nodes (substations) and links (transmission lines)

- **Global Power Plant Database**:
  - Detailed EU power plants data including ownership, capacity, and fuel types

### 🔗 **Relational Network Data**

- **ENTSO-E TSO Network**:
  - European Transmission System Operators (TSOs) and their interconnections

- **CORDIS EU Projects Database**:
  - Collaborative network of EU-funded energy research projects and organizations

### 📐 **Structured Outputs**

- **GeoJSON** for geospatial data
- **CSV** optimized for Neo4j import
- **JSON metadata** detailing sources, timestamps, and licensing

### 🛠️ **Flexible Configuration**

- Enable or disable datasets via environment flags (`RUN_OSM`, `RUN_GRIDKIT`, `RUN_POWERPLANTS`, `RUN_TSO_NETWORK`, `RUN_CORDIS`)

### 📑 **Robust Logging**

- Logs stored in `logs/geospatial_retrieval.log`

---

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
│   ├── osm
│   ├── gridkit
│   ├── powerplants
│   ├── tso_network
│   └── cordis
├── requirements.txt
└── scripts
    ├── osm_retrieval.py
    ├── gridkit_retrieval.py
    ├── powerplants_retrieval.py
    ├── tso_network_retrieval.py
    └── cordis_retrieval.py
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
RUN_GRIDKIT=1
RUN_POWERPLANTS=1
RUN_TSO_NETWORK=1
RUN_CORDIS=1
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
├── output/osm/neo4j_import/
│   ├── chargingstations_nodes.csv
│   ├── chargingstations_located_in_relationships.csv
│   ├── powerplants_nodes.csv
│   ├── powerplants_located_in_relationships.csv
│   ├── solarfarms_nodes.csv
│   ├── solarfarms_located_in_relationships.csv
│   ├── windturbines_nodes.csv
│   ├── windturbines_located_in_relationships.csv
│   ├── substations_nodes.csv
│   ├── substations_located_in_relationships.csv
│   ├── transmissionlines_nodes.csv
│   ├── transmissionlines_located_in_relationships.csv
│   └── countries_nodes.csv   
├── gridkit/
│   ├── nodes.csv
│   ├── relationships.csv
│   └── metadata.json
├── powerplants/
│   ├── powerplants_nodes.csv
│   ├── relationships.csv
│   └── metadata.json
├── tso_network/
│   ├── tso_nodes.csv
│   ├── connections_relationships.csv
│   └── metadata.json
└── cordis/
    ├── projects_nodes.csv
    ├── organizations_nodes.csv
    ├── participated_in_relationships.csv
    └── metadata.json
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