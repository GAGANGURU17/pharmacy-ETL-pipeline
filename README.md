# Pharmacy ETL Pipeline

A comprehensive ETL (Extract, Transform, Load) pipeline for processing pharmacy data using PySpark.

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
```

## Features
- Synthetic data generation for testing and development
- Multi-source data extraction (CSV, JSON, Database)
- Advanced data cleaning and transformation
- Dimensional modeling for analytics
- Scalable data loading with error handling
- Comprehensive logging system
- Interactive analytics dashboard
- Full test coverage

## Prerequisites
- Python 3.8+
- Apache Spark 3.x
- Java 8+ (for Spark)
- Virtual environment (recommended)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd pharmacy-etl
```

2. Create and activate virtual environment:
```bash
python -m venv venv
.\venv\Scripts\activate  # Windows
```

3. Install dependencies:
```bash
pip install -e ".[dev,notebook]"
```

## Configuration
1. Copy example configuration:
```bash
copy config\config.example.py config\config.py
```

2. Update configuration parameters in `config.py`:
- Database credentials
- File paths
- Spark settings
- Logging preferences

## Usage

### Running the ETL Pipeline
```bash
python src/main.py
```

### Data Generation
```bash
python src/data_generators/generate_all.py
```

### Running Tests
```bash
pytest tests/
```

### Analytics Dashboard
1. Start Jupyter server:
```bash
jupyter notebook
```
2. Open `notebooks/analytics_dashboard.ipynb`

## Data Model
- **Fact Tables**
  - Sales transactions
  - Inventory movements
- **Dimension Tables**
  - Time
  - Customer
  - Product
  - Store
  - Employee

## Error Handling
- Comprehensive logging
- Error recovery mechanisms
- Data validation checks
- Transaction management

## Performance Optimization
- Parallel processing
- Data partitioning
- Caching strategies
- Resource management

## Contributing
1. Fork the repository
2. Create feature branch
3. Commit changes
4. Push to branch
5. Create Pull Request

## Testing
- Unit tests
- Integration tests
- Data quality tests
- Performance tests

## License
MIT License

## Support
For support and questions, please contact:
- Email: gaganguru94@gmail.com
- Issues: GitHub Issues

## Authors
- Gagan N

## Acknowledgments
- Apache Spark community
- Python community
- Contributors and testers

        
