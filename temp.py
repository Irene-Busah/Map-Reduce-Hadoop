# importing libraries
from mrjob.job import MRJob

class AverageTemp(MRJob):
    # defining the mapper
    def mapper(self, _, line):
        """
        Given weather data, compute the average temperature for each city.

        input data -> each row
        (key, value) -> (city, temp)
        """
        data = tuple(line.split())
        city, temp = data
        yield city, int(temp)

    def reducer(self, key, values):
        temp = list(values)
        avg = sum(temp) / len(temp)
        yield key, avg


if __name__ == '__main__':
    AverageTemp.run()
