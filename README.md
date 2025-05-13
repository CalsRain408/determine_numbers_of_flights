# MapReduce Flight Data Analysis

This project implements a MapReduce-like system in Python for analyzing flight data. The system demonstrates how to process and analyze large datasets using a MapReduce approach with multithreading.

## Project Structure

- `mapreduce_framework.py` - Core MapReduce framework implementation
- `data_parsers.py` - Parsers for the flight and airport data files
- `mapreduce_jobs.py` - Specific MapReduce jobs for flight data analysis
- `main.py` - Main application that runs the MapReduce jobs
- `data` - Directory for saving the data csv files
- `result` - Directory for saving results json file

## Data Format

The system processes two main data files:

### 1. Passenger Flight Data
Contains information about passenger flights between airports with the following format:
- Passenger ID (Format: XXXnnnnXXn where X is uppercase ASCII and n is a digit 0-9)
- Flight ID (Format: XXXnnnnX)
- From airport IATA/FAA code (Format: XXX)
- Destination airport IATA/FAA code (Format: XXX)
- Departure time (GMT) in Unix epoch time
- Total flight time in minutes (1-4 digits)

### 2. Airport Data
Contains information about airports with the following format:
- Airport Name (3-20 characters)
- Airport IATA/FAA code (3 characters)
- Latitude (floating point, 3-13 digits)
- Longitude (floating point, 3-13 digits)

## MapReduce Jobs

The system includes a simple MapReduce job:

**Most Frequent Flyers** - Determines the passenger(s) with the highest number of flights

## Usage

```bash
python main.py --passenger-file <path_to_passenger_data> --airport-file <path_to_airport_data> [options]
```

### Command-line Options

- `--passenger-file` - Path to the passenger data file (required)
- `--airport-file` - Path to the airport data file (required)
- `--output-dir` - Directory to save results (default: 'results')
- `--mappers` - Number of mapper threads (default: 4)
- `--reducers` - Number of reducer threads (default: 2)
- `--job` - Job to run (default: 'frequent'), easy to add other jobs in future
- `--top-n` - Number of top results to display (default: 1)

## Example Usage

```bash
# Find the top 5 most frequent flyers
python main.py --passenger-file AComp_Passenger_data.csv --airport-file airports.csv --job frequent --top-n 5
```

## MapReduce Implementation

The implementation follows these key MapReduce principles:

1. **Map Phase** - Processes input records and emits key-value pairs
2. **Shuffle Phase** - Groups key-value pairs by key
3. **Reduce Phase** - Processes the grouped data to produce the final output

The framework uses Python's threading module to parallelize the map and reduce operations, simulating a distributed MapReduce environment.

## Requirements

- Python 3.6 or higher
- Standard library modules only (no external dependencies)
