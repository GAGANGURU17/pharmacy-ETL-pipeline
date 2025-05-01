# Pharmacy ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for pharmacy data management, built with Python and PySpark. This project handles the complete data lifecycle from synthetic data generation to warehouse storage and analytics.

## Features
- Synthetic Data Generation
  
  - Customer profiles with demographics and insurance info
  - Inventory management with product details
  - Sales transactions and history
- ETL Pipeline
  
  - Data extraction from multiple sources
  - Advanced data transformation and cleaning
  - Optimized data warehouse loading
- Data Quality
  
  - Comprehensive validation checks
  - Null value handling
  - Duplicate detection
  - Data consistency verification
- Analytics
  
  - Interactive dashboards
  - Data exploration notebooks
  - Performance metrics
## Project Structure
```
pharmacy-etl/
├── config/              # Configuration files
│   ├── config.py       # Main configuration
│   └── logging_config.py# Logging setup
├── data/               # Data storage
│   ├── raw/           # Raw input data
│   ├── processed/     # Intermediate data
│   └── warehouse/     # Final transformed data
├── src/               # Source code
│   ├── data_generators/# Synthetic data generation
│   ├── extraction/    # Data extraction components
│   ├── transformation/# Data transformation logic
│   ├── loading/       # Data loading modules
│   ├── utils/         # Helper utilities
│   └── main.py        # Pipeline orchestration
├── notebooks/         # Jupyter notebooks
├── tests/            # Test suite
└── setup.py         # Package configuration

## Technologies
- Python 3.7+
- PySpark
- Pandas
- Faker (for synthetic data)
- Jupyter Notebooks
## Installation
```bash
git clone <repository-url>
cd pharmacy-etl
pip install -r requirements.txt
 ```

## Usage
1. Generate synthetic data:
```bash
python -m src.data_generators.generate_all
 ```
```

2. Run ETL pipeline:
```bash
python -m src.main
 ```

3. View analytics:
```bash
jupyter notebook notebooks/analytics_dashboard.ipynb
 ```
```

## Data Flow
1. Data Generation → data/raw/
   
   - Customer profiles
   - Inventory records
   - Sales transactions
2. Processing → data/processed/
   
   - Data cleaning
   - Validation
   - Transformation
3. Warehouse → data/warehouse/
   
   - Dimensional modeling
   - Analytics-ready datasets
## Contributing
1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request
## License
MIT License

## Contact
For questions and support, please open an issue in the repository.
