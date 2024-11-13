# PythonBot2-YFinance-DataMiner

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

⛏️ Executes Data Mining by leveraging the yfinance API and web scraping to gather, organize, and save historical stock data for backtesting. A powerful tool for streamlined data collection and financial research. 💾

## Directory Structure
- `assets`: Stores additional assets (e.g., symbol lists or configuration files).
- `data`: Contains subfolders organized by year with CSV files for each stock symbol.
- `logs`: Houses log files generated with daily rotation.
- `main.py`: Main script to execute data mining.
- `requirements.txt`: Lists project dependencies.
- `venv`: Virtual environment for the project.

## Installation
1. Clone this repository.
2. Run `pip install -r requirements.txt` to install dependencies.
3. Run `python main.py` to start data mining.

## Usage
- To adjust parameters like symbols, year, or log level, modify `main.py` or load them from an external configuration file.

## Features
- **Logging**: Logs are rotated daily and saved in the `logs` folder.
- **Data Fetching**: Retrieves stock data for specified symbols and year, saved in `data/year` as CSV.
- **Rate Limit Delay**: This parameter controls the delay (in seconds) between consecutive API requests to ensure compliance with rate limits. Adjust this value as needed to avoid exceeding the allowed request rate.
- **Error Handling**: Logs errors for failed downloads and skips existing data to avoid redundancy.

## Contributing
We welcome contributions! If you'd like to help improve the project, please take a look at the [Contributing Guidelines](CONTRIBUTING.md) for more information on how to get started.
For ideas on what you can work on, check out our [Future Improvements](FUTURE_IMPROVEMENTS.md) document.

## License
MIT License.
