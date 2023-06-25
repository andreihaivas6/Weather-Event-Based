import json
import os
import time

from multiprocessing import Pool
from typing import List, Dict

from src.generator.PublicationsGenerator import PublicationsGenerator


class PublicationsGeneratorParallel:
    def __init__(self, no_publications: int, no_instances: int = 1):
        self.no_publications = no_publications
        self.no_instances = no_instances
        self.duration = 0

    def generate(self) -> List[Dict]:
        start = time.time()

        result = self._generate()

        end = time.time()
        self.duration = round(end - start, 2)

        if not os.path.exists('data'):
            os.mkdir('data')

        with open('data/publications.json', 'w') as file:
            file.write(json.dumps(result, indent=4))

        with open('data/publications_test_data.json', 'w') as file:
            file.write(json.dumps(self.get_data_for_testing(), indent=4))

        return result

    def _generate(self) -> List[Dict]:
        no_publications_per_instance = self.no_publications // self.no_instances
        no_extra_publications = self.no_publications % self.no_instances

        if self.no_instances == 1:
            return PublicationsGenerator.generate(self.no_publications)
        else:
            publications = list()

            with Pool(self.no_instances) as pool:
                for result in pool.map(
                        PublicationsGenerator.generate,
                        [no_publications_per_instance + no_extra_publications] +
                        [no_publications_per_instance] * (self.no_instances - 1)
                ):
                    publications.extend(result)

            return publications

    def get_data_for_testing(self) -> dict:
        return {
            'no_publications': self.no_publications,
            'no_instances': self.no_instances,
            'duration (sec)': self.duration,
        }
