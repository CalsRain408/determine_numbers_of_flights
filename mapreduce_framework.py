"""
MapReduce Framework - Core Implementation

This module provides the base classes and functionality for a MapReduce-like system
to process flight and airport data.
"""
import logging
import threading
import queue
import time
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Tuple, Callable, TypeVar, Generic, Iterable
import csv
from datetime import datetime


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Type variables for generics
K = TypeVar('K')  # Key type
V = TypeVar('V')  # Value type
R = TypeVar('R')  # Result type

class Mapper(Generic[K, V], ABC):
    """
    Abstract base class for mappers.
    """

    @abstractmethod
    def map(self, key: Any, value: Any) -> Iterable[Tuple[K, V]]:
        """
        Map function to be implemented by concrete mappers.
        :param key: The input key.
        :param value: The input value.
        :return: An iterable of key-value pairs.
        """
        pass

class Reducer(Generic[K, V, R], ABC):
    """
    Abstract base class for reducers.
    """

    @abstractmethod
    @abstractmethod
    def reduce(self, key: K, values: Iterable[V]) -> Iterable[R]:
        """
        Reduce function to be implemented by concrete reducers.
        :param key: The key for the group.
        :param values: The values associated with the key.
        :return: An iterable of results.
        """
        pass

class MapReduceFramework:
    """
    A simple MapReduce framework implementation.
    """

    def __init__(self, num_mappers: int = 4, num_reducers: int = 2):
        """
        Initialize a MapReduce Framework.
        :param num_mappers: Number of mapper threads.
        :param num_reducers: Number of reducer threads.
        """
        self.num_mappers = num_mappers
        self.num_reducers = num_reducers
        self.map_queue = queue.Queue()
        self.shuffle_dict: Dict[K, List[V]] = {}
        self.shuffle_lock = threading.Lock()
        self.reduce_queue = queue.Queue()
        self.results = []
        self.results_lock = threading.Lock()

    def run(self, input_data: List[Tuple[Any, Any]],
            mapper: Mapper,
            reducer: Reducer) -> List[Any]:
        """
        Run a MapReduce job.
        :param input_data: List of key-value pairs to process.
        :param mapper: Mapper implementation.
        :param reducer: Reducer implementation.
        :return: List of results from the reducers.
        """
        # Reset state
        self.map_queue = queue.Queue()
        self.shuffle_dict = {}
        self.reduce_queue = queue.Queue()
        self.results = []

        # Load data into map queue
        for key, value in input_data:
            self.map_queue.put((key, value))

        # Create and start mapper threads
        logger.info(f"Starting {self.num_mappers} mapper threads")
        map_threads = []
        for i in range(self.num_mappers):
            t = threading.Thread(
                target=self._map_worker,
                args=(mapper,),
                name=f"Mapper-{i}"
            )
            map_threads.append(t)
            t.start()

        # Wait for mappers to finish
        for t in map_threads:
            t.join()
        logger.info("All mappers finished")

        # Perform shuffling
        self._shuffle()

        # Create and start reducer threads
        logger.info(f"Starting {self.num_reducers} reducer threads")
        reduce_threads = []
        for i in range(self.num_reducers):
            t = threading.Thread(
                target=self._reduce_worker,
                args=(reducer,),
                name=f"Reducer-{i}"
            )
            reduce_threads.append(t)
            t.start()

        # Wait for reducers to finish
        for t in reduce_threads:
            t.join()
        logger.info("All reducers finished")

        return self.results

    def _map_worker(self, mapper: Mapper) -> None:
        """
        Worker function for mapper threads.
        :param mapper: The mapper implementation to use.
        :return:
        """
        while not self.map_queue.empty():
            try:
                key, value = self.map_queue.get(block=False)
                for map_key, map_value in mapper.map(key, value):
                    with self.shuffle_lock:
                        if map_key not in self.shuffle_dict:
                            self.shuffle_dict[map_key] = []
                        self.shuffle_dict[map_key].append(map_value)
                self.map_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error in reducer: {e}")

    def _shuffle(self) -> None:
        """
        Perform the shuffle operation by grouping key-value pairs.
        """
        logger.info("Performing shuffle operation")
        for key, values in self.shuffle_dict.items():
            self.reduce_queue.put((key, values))

    def _reduce_worker(self, reducer: Reducer) -> None:
        """
        Worker function for reducer threads.
        :param reducer: The reducer implementation to use.
        """
        while not self.reduce_queue.empty():
            try:
                key, values = self.reduce_queue.get(block=False)
                for result in reducer.reduce(key, values):
                    with self.results_lock:
                        self.results.append(result)
                self.reduce_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error in reducer: {e}")