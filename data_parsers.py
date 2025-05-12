"""
Data Parsers for Flight and Airport Data

This module provides parsers for the flight passenger data and airport data files.
"""
import csv
import re
from typing import Dict, List, Any, Tuple, Optional
import logging
from datetime import datetime


logger = logging.getLogger(__name__)

class PassengerDataParser:
    """
    Parser for the passenger flight data file.
    """

    # Regular expression for validating fields
    PASSENGER_ID_PATTERN = re.compile(r'^[A-Z]{3}[0-9]{4}[A-Z]{2}[0-9]$')
    FLIGHT_ID_PATTERN = re.compile(r'^[A-Z]{3}[0-9]{4}[A-Z]$')
    AIRPORT_CODE_PATTERN = re.compile(r'^[A-Z]{3}$')

    def __init__(self, file_path: str):
        """
        Initialize the parser with a file path.
        :param file_path: Path to the passenger data CSV file.
        """
        self.file_path = file_path

    def parse(self) -> List[Dict[str, Any]]:
        """
        Parse the passenger data file.
        :return: A list of dictionaries, each representing a passenger record.
        """
        records = []

        try:
            with open(self.file_path, 'r', newline='') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if len(row) != 6:
                        logger.warning(f"Line {i+1} has wrong number of fields: {len(row)}")
                        continue

                    passenger_id, flight_id, from_airport, dest_airport, departure_time, flight_time = row

                    # Validate fields
                    if not self._validate_passenger_record(
                        i+1, passenger_id, flight_id, from_airport, dest_airport, departure_time, flight_time
                    ):
                        continue

                    # Parse fields
                    try:
                        departure_time_int = int(departure_time)
                        flight_time_int = int(flight_time)
                    except ValueError:
                        logger.warning(f"Line {i+1} has invalid numeric values")
                        continue

                    # Create record
                    record = {
                        'passenger_id': passenger_id,
                        'flight_id': flight_id,
                        'from_airport': from_airport,
                        'dest_airport': dest_airport,
                        'departure_time': departure_time,
                        'flight_time': flight_time,
                        'departure_datetime': datetime.fromtimestamp(departure_time_int)
                    }
                    records.append(record)

        except FileNotFoundError:
            logger.error(f"File not found: {self.file_path}")
        except Exception as e:
            logger.error(f"Error parsing passenger data: {e}")

        logger.info(f"Parsed {len(records)} passenger records")
        return records

    def _validate_passenger_record(self, line_num: int,
                                   passenger_id: str,
                                   flight_id: str,
                                   from_airport: str,
                                   dest_airport: str,
                                   departure_time: str,
                                   flight_time: str
                                   ) -> bool:
        """
        Validate a passenger record.
        :param line_num: The line number for error reporting.
        :param passenger_id:
        :param flight_id:
        :param from_airport:
        :param dest_airport:
        :param departure_time: Departure time (Unix epoch).
        :param flight_time: in minutes
        :return: True if the record is valid, False otherwise.
        """
        # Validate passenger ID
        if not self.PASSENGER_ID_PATTERN.match(passenger_id):
            logger.warning(f"Line {line_num}: Invalid passenger ID: {passenger_id}")
            return False

        # Validate flight ID
        if not self.FLIGHT_ID_PATTERN.match(flight_id):
            logger.warning(f"Line {line_num}: Invalid flight ID: {flight_id}")
            return False

        # Validate airport codes
        if not self.AIRPORT_CODE_PATTERN.match(from_airport):
            logger.warning(f"Line {line_num}: Invalid from airport code: {from_airport}")
            return False

        if not self.AIRPORT_CODE_PATTERN.match(dest_airport):
            logger.warning(f"Line {line_num}: Invalid destination airport code: {dest_airport}")
            return False

        # Validate departure time
        try:
            departure_time_int = int(departure_time)
            if departure_time_int < 0:
                logger.warning(f"Line {line_num}: Negative departure time: {departure_time}")
                return False
        except ValueError:
            logger.warning(f"Line {line_num}: Invalid departure time: {departure_time}")
            return False

        # Validate flight time (1-4 digits)
        try:
            flight_time_int = int(flight_time)
            if flight_time_int < 1 or flight_time_int > 9999:
                logger.warning(f"Line {line_num}: Flight time out of range: {flight_time}")
                return False
        except ValueError:
            logger.warning(f"Line {line_num}: Invalid flight time: {flight_time}")
            return False

        return True


class AirportDataParser:
    """
    Parser for the airport data file.
    """

    # Regular expression for validating fields
    AIRPORT_CODE_PATTERN = re.compile(r'^[A-Z]{3}$')
    AIRPORT_NAME_PATTERN = re.compile(r'^.{3,20}$')
    COORDINATE_PATTERN = re.compile(r'^-?\d+(\.\d{1,13})?$')

    def __init__(self, file_path: str):
        """
        Initialize the parser with a file path.
        :param file_path: Path to the airport data CSV file.
        """
        self.file_path = file_path

    def parse(self) -> Dict[str, Dict[str, Any]]:
        """
        Parse the airport data file.
        :return: A dictionary mapping airport codes to airport information.
        """
        airports = {}

        try:
            with open(self.file_path, 'r', newline='') as f:
                reader = csv.reader(f)
                for i, row in enumerate(reader):
                    if len(row) != 4:
                        logger.warning(f"Line {i+1} has wrong number of fields: {len(row)}")
                        continue
                    name, code, latitude, longitude = row

                    # Validate fields
                    if not self._validate_airport_record(i+1, name, code, latitude, longitude):
                        continue

                    # Parse fields
                    try:
                        lat_float = float(latitude)
                        lon_float = float(longitude)
                    except ValueError:
                        logger.warning(f"Line {i+1} has invalid coordinate values")
                        continue

                    # Create record
                    airports[code] = {
                        'name': name,
                        'code': code,
                        'latitude': lat_float,
                        'longitude': lon_float
                    }

        except FileNotFoundError:
            logger.error(f"File not found: {self.file_path}")
        except Exception as e:
            logger.error(f"Error parsing airport data: {e}")

        logger.info(f"Parsed {len(airports)} airport records")
        return airports

    def _validate_airport_record(self, line_num: int,
                                 name: str, code: str,
                                 latitude: str, longitude: str
                                 ) -> bool:
        """
        Validate an airport record.
        :param line_num: The line number for error reporting.
        :param name:
        :param code: Airport IATA/FAA code.
        :param latitude:
        :param longitude:
        :return: True if the record is valid, False otherwise.
        """
        # Validate airport name
        if not self.AIRPORT_NAME_PATTERN.match(name):
            logger.warning(f"Line {line_num}: Invalid airport name: {name}")
            return False

        # Validate airport code
        if not self.AIRPORT_CODE_PATTERN.match(code):
            logger.warning(f"Line {line_num}: Invalid airport code: {code}")
            return False

        # Validate coordinates
        if not self.COORDINATE_PATTERN.match(latitude):
            logger.warning(f"Line {line_num}: Invalid latitude: {latitude}")
            return False

        if not self.COORDINATE_PATTERN.match(longitude):
            logger.warning(f"Line {line_num}: Invalid longitude: {longitude}")
            return False

        # Check latitude range (-90 to 90) and longitude range (-180 to 180)
        try:
            lat_float = float(latitude)
            if lat_float < -90 or lat_float > 90:
                logger.warning(f"Line {line_num}: Latitude out of range: {latitude}")
                return False

        except ValueError:
            logger.warning(f"Line {line_num}: Invalid latitude value: {latitude}")
            return False

        try:
            lon_float = float(longitude)
            if lat_float < -180 or lat_float > 180:
                logger.warning(f"Line {line_num}: Longitude out of range: {longitude}")
                return False

        except ValueError:
            logger.warning(f"Line {line_num}: Invalid longitude value: {longitude}")
            return False

        return True
