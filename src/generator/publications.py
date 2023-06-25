import json

from PublicationsGeneratorParallel import PublicationsGeneratorParallel

NO_PUBLICATIONS = 99_999
NO_PARALLEL_INSTANCES = 4

if __name__ == '__main__':
    generator = PublicationsGeneratorParallel(NO_PUBLICATIONS, NO_PARALLEL_INSTANCES)
    publications = generator.generate()

    print(json.dumps(generator.get_data_for_testing(), indent=4))


