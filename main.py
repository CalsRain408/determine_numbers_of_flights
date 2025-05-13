"""
Main Application for MapReduce Flight Data Analysis

This script runs the MapReduce jobs to analyze flight data

Show the top n most frequent flyers
"""
import argparse
import logging
import time
import json
from typing import Dict, List, Any, Tuple
import os

from mapreduce_framework import MapReduceFramework
from data_parsers import PassengerDataParser, AirportDataParser
from mapreduce_jobs import (PassengerFlightCountMapper, PassengerFlightCountReducer)


# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data(passenger_file: str, airport_file: str) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Load and parse the data files.
    :param passenger_file: Path to the passenger data file.
    :param airport_file: Path to the airport data file.
    :return: Tuple of (passenger_records, airport)
    """
    logger.info(f"Loading passenger data from {passenger_file}")
    passenger_parser = PassengerDataParser(passenger_file)
    passenger_records = passenger_parser.parse()

    logger.info(f"Loading airport data from {airport_file}")
    airport_parser = AirportDataParser(airport_file)
    airports = airport_parser.parse()

    return passenger_records, airports


def prepare_input_data(passenger_records: List[Dict[str, Any]]) -> List[Tuple[int, Dict[str, Any]]]:
    """
    Prepare input data for MapReduce framework.
    :param passenger_records: List of passenger records.
    :return: List of (index, record) tuples.
    """
    return [(i, record) for i, record in enumerate(passenger_records)]


def print_results(job_name: str, results: List[Any], top_n: int = 10) -> None:
    """
    Print job results to console in a formatted way.
    :param job_name: Name of the job.
    :param results: List of job results.
    :param top_n: Number of top results to display (for jobs that rank results).
    """

    print(f"\n=== {job_name} Results ===")

    if job_name == "Most Frequent Flyers":
        # Sort passengers by flight count
        sorted_results = sorted(results, key=lambda x : x[1], reverse=True)

        print(f"Top {top_n} Most Frequent Flyers:")
        print(f"{'Rank':6} {'Passenger ID':15} {'Flight Count':12}")
        print("-" * 36)

        # Show the top N passengers
        for i, (passenger_id, flight_count) in enumerate(sorted_results[:top_n]):
            print(f"{i + 1:6} {passenger_id:15} {flight_count:12}")

        # Count total passengers
        total_passengers = len(results)
        if total_passengers > top_n:
            print(f"\nTotal number of passengers: {total_passengers}")

    else:
        print(f"Invalid job name: {job_name}, please try again.")


def export_results(job_name: str, results: List[Any], output_dir: str) -> None:
    """
    Export job results to JSON files.
    :param job_name:
    :param results:
    :param output_dir: Directory to save output files.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Clean up job name for filename
    filename = job_name.lower().replace(' ', '_') + '.json'
    filepath = os.path.join(output_dir, filename)

    # Convert results to a more JSON-friendly format
    json_results = []

    for key, value in results:
        if isinstance(key, tuple):
            result = {'key': list(key), 'value': value}
        else:
            result = {'key': key, 'value': value}
        json_results.append(result)

    with open(filepath, 'w') as f:
        json.dump(json_results, f, indent=2)

    logger.info(f"Exported results to {filepath}")


def run_job(framework: MapReduceFramework, job_name: str, input_data: List[Tuple[Any, Any]],
            mapper: Any, reducer: Any, output_dir: str, top_n: int = 10) -> None:
    """
    Run a MapReduce job and handle results.
    :param framework: The MapReduce framework instance.
    :param job_name: Name of the job.
    :param input_data: Input data for the job.
    :param mapper: Mapper implementation.
    :param reducer: Reducer implementation.
    :param output_dir: Directory to save output files.
    :param top_n: Number of top results to display.
    """
    logger.info(f"Running job {job_name}")
    start_time = time.time()

    results = framework.run(input_data, mapper, reducer)

    elapsed_time = time.time() - start_time
    logger.info(f"{job_name} job completed in {elapsed_time:.2f} seconds")

    print_results(job_name, results, top_n)
    export_results(job_name, results, output_dir)


def main():
    """
    Main function to run the MapReduce analysis.
    """
    parser = argparse.ArgumentParser(description='MapReduce Flight Data Analysis')
    parser.add_argument('--passenger-file', default='', required=True, help='Path to passenger data file')
    parser.add_argument('--airport-file', default='', required=True, help='Path to airport data file')
    parser.add_argument('--output-dir', default='./results', help='Directory to save results')
    parser.add_argument('--mappers', type=int, default=4, help='Number of mapper threads')
    parser.add_argument('--reducers', type=int, default=2, help='Number of reducer threads')
    parser.add_argument('--job', default='frequent', help='Job to run')
    parser.add_argument('--top-n', type=int, default=1, help='Number of top results to display (default: 1)')

    args = parser.parse_args()

    # Load data
    passenger_records, airports = load_data(args.passenger_file, args.airport_file)

    if not passenger_records:
        logger.error("No passenger records found. Exiting.")
        return

    if not airports:
        logger.error("No airport records found. Some jobs may not work correctly.")

    # Prepare input data
    input_data = prepare_input_data(passenger_records)

    # Create MapReduce framework
    framework = MapReduceFramework(num_mappers=args.mappers, num_reducers=args.reducers)

    # Run selected jobs
    if args.job == 'frequent':
        run_job(framework, "Most Frequent Flyers", input_data,
                PassengerFlightCountMapper(), PassengerFlightCountReducer(),
                args.output_dir, args.top_n)

    logger.info("All jobs completed successfully")


if __name__ == '__main__':
    main()