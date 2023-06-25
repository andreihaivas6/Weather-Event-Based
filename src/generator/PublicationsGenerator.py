import datetime
import random

from typing import List, Dict


class PublicationsGenerator:
    CITIES = [
        'Bucharest', 'Iasi', 'Cluj-Napoca', 'Timisoara', 'Constanta', 'Dragalina', 'Roman'
    ]
    DIRECTIONS = [
        'N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'
    ]

    @staticmethod
    def generate(no_of_publications) -> List[Dict]:
        publications = list()

        for _ in range(no_of_publications):
            publication = dict()

            publication["stationId"] = random.randint(0, 100)
            publication["city"] = random.choice(PublicationsGenerator.CITIES)
            publication["temperature"] = random.randint(-20, 50)
            publication["rain"] = round(random.random(), 2)
            publication["wind"] = random.randint(0, 90)
            publication["direction"] = random.choice(PublicationsGenerator.DIRECTIONS)
            publication["date"] = PublicationsGenerator.get_random_date()

            publications.append(publication)

        return publications

    @staticmethod
    def get_random_date() -> str:
        start = datetime.datetime.strptime('1/1/2000', '%d/%m/%Y')
        end = datetime.datetime.strptime('1/1/2023', '%d/%m/%Y')
        result = start + datetime.timedelta(
            seconds=random.randint(0, int((end - start).total_seconds())),
        )
        return result.strftime('%d/%m/%Y')
