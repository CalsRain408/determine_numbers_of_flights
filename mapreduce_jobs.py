"""
MapReduce Jobs for Flight Data Analysis

This module provides specific MapReduce jobs for analyzing flight data.
"""
from typing import Dict, List, Any, Tuple, Iterable
import logging

from mapreduce_framework import Mapper, Reducer, K, V, R

logger = logging.getLogger(__name__)

class PassengerFlightCountMapper(Mapper):
    """
    Mapper for counting the number of flights per passenger.
    """

    def map(self, _, record: Dict[str, Any]) -> Iterable[Tuple[str, int]]:
        """
        Map function to emit a count for each passenger.
        :param _: Unused key.
        :param record: A passenger record.
        :return: Key-Value pairs where keys are passengers' ID and values are counts.
        """
        passenger_id = record['passenger_id']
        yield (passenger_id, 1)


class PassengerFlightCountReducer(Reducer):
    """
    Reducer for counting the total number of flights per passenger.
    """

    def reduce(self, key: str, values: Iterable[int]) -> Iterable[Tuple[str, int]]:
        """
        Reduce Function to sum up flights per passenger.
        :param key: Passenger ID.
        :param values: List of counts.
        :return: Passenger ID and total flight count.
        """
        total_flights = sum(values)
        yield (key, total_flights)