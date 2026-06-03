# Planned Features & Products

This page documents features and data products planned for or under active
development in GLHE.

## Web Dashboard

The LIME dashboard (built on Plotly/Dash) is planned to include:

- User-friendly lake selection interface
- Summary and overview section per lake
- Interactive charts, graphs, and maps for all water balance components
- Customization options (parameter selection, time range)
- Real-time updates where data availability permits
- Download and export functionality
- Contextual explanations for each data product
- Responsive design for different screen sizes

## Information & Data Displayed

- Lake name, location, area, and depth
- Historical lake parameters (water level, temperature, etc.)
- Historical trends and patterns
- Weather and climate data (temperature, precipitation, wind speed)
- Ecological and environmental factors
- Hydrological data (inflow, outflow, streamflow)
- Historical and projected water levels

## Downloadable Data

- Raw data files (CSV, JSON, NetCDF)
- Aggregated data (hourly, daily, weekly, monthly averages)
- Metadata files (data source, variables, collection methods)
- Sample code and scripts for data retrieval and processing
- Documentation (data characteristics, usage instructions)

## Intermediary Products

### Uncertainty and Cautionary Flags

An API or indicator for identifying areas with potential data weaknesses.
Provides information on data quality, gaps, and uncertainties — particularly
important given the multi-source nature of GLHE data.

### Global Product API

An API providing information on global data products (satellite data, climate
indices) suitable for the selected lake's location. Helps users identify
relevant global datasets for comparison and analysis.

### Visualizer API

Low-priority feature: allow users to upload their own CSV data and visualize
it alongside GLHE outputs on the dashboard.
