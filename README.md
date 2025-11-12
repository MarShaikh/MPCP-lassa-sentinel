# Microsoft Planetary Computer Pipelines 

A scalable, cloud-based geospatial data processing pipeline for ingesting and processing CHIRPS climate data in it's initial instance and MODIS satellite imagery using Azure Batch and Azure GeoCatalog. This project enables automated conversion of geospatial raster data to Cloud Optimized GeoTIFFs (COGs) with STAC (SpatioTemporal Asset Catalog) metadata generation for efficient discovery and visualization.

## Overview

This project supports two distinct data ingestion workflows:

### Workflow 1: Direct Ingestion from Planetary Computer
A straightforward search-and-load process:
1. **Search** - Query Microsoft Planetary Computer's STAC catalog for available data
2. **Validate** - Check and fix STAC metadata if needed
3. **Ingest** - Directly ingest STAC items into your Azure GeoCatalog


### Workflow 2: CHIRPS Data Processing Pipeline
A full processing pipeline for raw geospatial data:
1. **Extract** - Scrape CHIRPS precipitation data URLs from UCSB
2. **Download** - Retrieve raw GeoTIFF files
3. **Convert to COGs** - Transform to Cloud Optimized GeoTIFFs using Azure Batch
4. **Generate STAC** - Create STAC metadata items with proper geospatial indexing
5. **Ingest** - Upload to Azure GeoCatalog for visualization and discovery

This workflow leverages **Azure Batch for distributed processing** of thousands of files.

## Key Features

- **Two Ingestion Modes**: Simple direct ingestion from Planetary Computer OR complex batch processing for raw data
- **Scalable Processing**: Azure Batch integration for distributed processing of large datasets (CHIRPS pipeline)
- **Cloud-Optimized**: Generates COGs for efficient cloud-native geospatial workflows (CHIRPS pipeline)
- **STAC Compliant**: Full STAC metadata generation and validation for both workflows
- **Automated Ingestion**: Direct integration with Microsoft Planetary Computer and Azure GeoCatalog
- **Progress Monitoring**: Real-time tracking of batch jobs (CHIRPS pipeline) and ingestion status
- **Error Handling**: Automatic validation and correction of STAC metadata errors
- **Flexible Configuration**: Support for multiple regions, date ranges, and data sources

## Architecture

The system is organized into modular components:

- **COG Creation Pipeline** (`src/cog_creation/`) - Extract and convert raw data to COGs
- **STAC Creation Pipeline** (`src/stac_creation/`) - Generate STAC metadata and catalog
- **Ingestion** (`src/ingestion/`) - Import data from external sources (Planetary Computer)
- **Shared Utilities** (`src/utils/`) - Common Azure Batch and Storage operations

### Processing Flow

**WORKFLOW 1: Direct Ingestion from Planetary Computer**

```
┌─────────────────────────┐
│ Microsoft Planetary     │
│ Computer                │
│ (STAC Catalog)          │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Search STAC API         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Validate Items          │
│ (Fix metadata)          │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│ Azure GeoCatalog        │
│ (Direct Ingestion)      │
└─────────────────────────┘
```

**WORKFLOW 2: CHIRPS Data Processing Pipeline**

```
┌─────────────────────────┐
│ CHIRPS Data             │
│ (UCSB)                  │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ Azure Batch Operation 1                                 │
│ (Extract URLs → Download TIFFs → Convert to COGs)       │
└───────────┬─────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────┐
│ Azure Blob Storage      │
│ (processed-cogs)        │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ Azure Batch Operation 2                                 │
│ (Generate STAC Items)                                   │
└───────────┬─────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────┐
│ Azure Blob Storage      │
│ (stac-items)            │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────┐
│ Bulk Ingestion Script                                   │
└───────────┬─────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────┐
│ Azure GeoCatalog        │
│ (Bulk Ingestion)        │
└─────────────────────────┘
```

## Quick Start

### Prerequisites

**For Both Workflows:**
- Azure subscription with Azure GeoCatalog instance
- Python 3.11+
- Conda environment manager
- Azure CLI

**For CHIRPS Pipeline Only (Workflow 2):**
- Azure Batch account
- Azure Storage account with blob containers
- Azure Key Vault
- Service principal with appropriate permissions

### Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd MPCP_lassa_sentinel
   ```

2. Create and activate conda environment:
   ```bash
   conda create -n MPCP_lassasentinel python=3.11 -y
   conda activate MPCP_lassasentinel
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   ```bash
   cp configs/config.env.example configs/config.env
   # Edit config.env with your Azure credentials
   source configs/config.env
   ```

### Basic Usage

#### Ingest Data from Planetary Computer

```bash
python src/ingestion/ingestion_from_datacatalog.py \
    --geocatalog-url "https://your-catalog.geocatalog.spatio.azure.com" \
    --pc-collection "modis-13Q1-061" \
    --bbox-aoi 2.316388 3.837669 15.126447 14.153350 \
    --date-range "2020-01-01/2020-12-31" \
    --region "nigeria" \
    --batch-size 100
```

#### Run COG Creation Pipeline

```bash
python src/cog_creation/batch_job_creator.py
python src/cog_creation/progress_monitor.py
```

#### Create STAC Catalog and Ingest

```bash
python src/stac_creation/orchestrate_catalog_ingestion.py \
    --geocatalog-uri "https://your-catalog.geocatalog.spatio.azure.com" \
    --collection-id "Nigeria-CHIRPS" \
    --collection-title "Nigeria CHIRPS Collection" \
    --collection-desc "CHIRPS v2.0 precipitation data for Nigeria" \
    --bbox 2.316388 3.837669 15.126447 14.153350
```

## Project Structure

```
MPCP_lassa_sentinel/
├── src/
│   ├── cog_creation/           # COG processing pipeline
│   │   ├── batch_job_creator.py
│   │   ├── batch_task_runner.py
│   │   ├── progress_monitor.py
│   │   ├── data_extraction.py
│   │   └── processing.py
│   ├── stac_creation/          # STAC metadata generation
│   │   ├── batch_job_creator.py
│   │   ├── batch_task_runner.py
│   │   ├── progress_monitor.py
│   │   ├── stac_conversion.py
│   │   ├── define_catalog.py
│   │   └── orchestrate_catalog_ingestion.py
│   ├── ingestion/              # External data ingestion
│   │   └── ingestion_from_datacatalog.py
│   └── utils/                  # Shared utilities
│       ├── azure_batch_utils.py
│       ├── azure_storage_utils.py
│       ├── base_progress_monitor.py
│       └── batch_task_utils.py
├── configs/                    # Configuration files
├── docs/                       # Comprehensive documentation
│   ├── SETUP.md
│   ├── BATCH_AND_PROCESSING.md
│   ├── TESTING.md
│   └── TROUBLESHOOTING.md
├── test_suite/                 # Unit and integration tests
├── notebooks/                  # Jupyter notebooks for exploration
└── CLAUDE.md                   # Project architecture guide
```

## Documentation

Comprehensive guides are available in the [`docs/`](./docs/) directory:

- **[SETUP.md](./docs/SETUP.md)** - Complete setup instructions for Azure, GeoCatalog, Storage, and Authentication
- **[BATCH_AND_PROCESSING.md](./docs/BATCH_AND_PROCESSING.md)** - Azure Batch configuration, ingestion sources, and processing pipelines
- **[TESTING.md](./docs/TESTING.md)** - Running unit and integration tests with Azurite
- **[TROUBLESHOOTING.md](./docs/TROUBLESHOOTING.md)** - Common issues and solutions
- **[CLAUDE.md](./CLAUDE.md)** - Detailed project architecture and design patterns

## Main Workflows

### Workflow 1: Direct Ingestion from Planetary Computer (Simple)

**Description:** Search Microsoft Planetary Computer's STAC catalog and directly ingest items into your Azure GeoCatalog. This workflow is lightweight and doesn't require Azure Batch or COG conversion since Planetary Computer data is already cloud-optimized.

**Use Cases:**
- Ingesting MODIS data (already in COG format)
- Loading Sentinel-2, Landsat, or other Planetary Computer collections
- Quick data availability checks for specific regions/dates

**Key Files:**
- `src/ingestion/ingestion_from_datacatalog.py` - MPC to GeoCatalog ingestion with STAC validation

**Azure Resources Needed:**
- Azure GeoCatalog instance only (no Batch or Storage required)

---

### Workflow 2: CHIRPS Data Processing Pipeline (Complex)

**Description:** Full processing pipeline for raw CHIRPS precipitation data. Extracts data from UCSB, converts to COGs using Azure Batch for parallel processing, generates STAC metadata, and ingests into GeoCatalog.

**Use Cases:**
- Processing CHIRPS precipitation data from UCSB
- Converting any raw GeoTIFF data to cloud-optimized format
- Large-scale batch processing of thousands of files

**Pipeline Stages:**

#### Stage 1: COG Creation
Extracts CHIRPS precipitation data URLs from UCSB and converts them to Cloud Optimized GeoTIFFs using Azure Batch for parallel processing.

**Key Files:**
- `src/cog_creation/data_extraction.py` - Web scraping CHIRPS URLs
- `src/cog_creation/batch_job_creator.py` - Batch job orchestration
- `src/cog_creation/batch_task_runner.py` - Executed on Batch nodes
- `src/cog_creation/progress_monitor.py` - Real-time progress tracking

#### Stage 2: STAC Metadata Generation
Converts COG files to STAC items with proper metadata including spatial extent, temporal coverage, and asset references.

**Key Files:**
- `src/stac_creation/stac_conversion.py` - COG to STAC conversion
- `src/stac_creation/batch_job_creator.py` - Batch job for STAC generation
- `src/stac_creation/batch_task_runner.py` - Executed on Batch nodes
- `src/stac_creation/define_catalog.py` - STAC catalog generation
- `src/stac_creation/orchestrate_catalog_ingestion.py` - Catalog creation and bulk ingestion

**Azure Resources Needed:**
- Azure GeoCatalog instance
- Azure Batch account (for distributed processing)
- Azure Storage account (5 containers)
- Azure Key Vault
- Service principal

## Storage Containers

**Note:** Storage containers are only required for Workflow 2 (CHIRPS Pipeline). Workflow 1 (Planetary Computer) doesn't need Azure Storage.

The CHIRPS processing pipeline uses five Azure Blob Storage containers:

- `raw-data` - Original downloaded TIFF files
- `processed-cogs` - Cloud Optimized GeoTIFFs
- `stac-items` - STAC JSON metadata files
- `task-data` - Work item JSON files for batch tasks
- `batch-logs` - Progress tracking files

## Testing

Run the test suite from the `test_suite/` directory:

```bash
cd test_suite

# All tests with coverage
make test-all

# Unit tests only (fast)
make test-unit

# Integration tests (requires Azurite)
make test-integration
```

See [TESTING.md](./docs/TESTING.md) for detailed testing instructions.

## Requirements

### Azure Resources

**For Workflow 1 (Planetary Computer Ingestion):**
- Azure GeoCatalog instance (canadacentral, northcentralus, or westeurope)

**For Workflow 2 (CHIRPS Pipeline) - Additional Requirements:**
- Azure Batch account (User Subscription mode)
- Azure Storage account with 5 blob containers
- Azure Key Vault
- Service principal with appropriate permissions

### Python Packages

Key dependencies (see `requirements.txt` for full list):

- `azure-storage-blob>=12.19.0`
- `azure-batch>=14.0.0`
- `azure-identity>=1.15.0`
- `rasterio>=1.3.0`
- `pystac>=1.9.0`
- `planetary-computer>=1.0.0`
- `requests>=2.31.0`
- `beautifulsoup4>=4.12.0`

## Configuration

Environment variables are stored in `configs/config.env`:

```bash
# Azure Batch
export AZURE_TENANT_ID="your-tenant-id"
export AZURE_CLIENT_ID="your-client-id"
export AZURE_CLIENT_SECRET="your-client-secret"
export BATCH_ACCOUNT_URL="https://your-batch.batch.azure.com"

# Azure Storage
export STORAGE_ACCOUNT_URL="https://your-storage.blob.core.windows.net"
export BATCH_STORAGE_ACCOUNT_KEY="your-storage-key"
```

See [SETUP.md](./docs/SETUP.md) for detailed configuration instructions.

## Troubleshooting

Common issues and solutions are documented in [TROUBLESHOOTING.md](./docs/TROUBLESHOOTING.md), including:

- GeoCatalog location and naming requirements
- Azure Batch authentication issues
- Storage access and SAS token errors
- STAC validation failures
- Performance optimization tips
- Visualization and rendering issues

## Support

For issues or questions:

1. Check the [Troubleshooting Guide](./docs/TROUBLESHOOTING.md)
2. Review [Setup Documentation](./docs/SETUP.md)
3. Consult Azure documentation:
   - [Microsoft Planetary Computer](https://learn.microsoft.com/en-us/azure/planetary-computer/)
   - [Azure Batch](https://learn.microsoft.com/en-us/azure/batch/)
   - [Azure Storage](https://learn.microsoft.com/en-us/azure/storage/)
4. Check Azure service health: https://status.azure.com/

