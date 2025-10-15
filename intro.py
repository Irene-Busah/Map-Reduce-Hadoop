# importing libraries
from mrjob.step import MRStep
from mrjob.job import MRJob


# defining the mrjob clss

class WordCount(MRJob):
    # defining the mapper function

    def mapper(self, _, line):
        """
        Given a text file, count how many times each work appears
        
        Input data -> each line of text
        (key, value) -> word, value count
        """
        words = line.split()
        for word in words:
            yield word, 1

    def reducer(self, key, values):
        """Receives the (key, value) and yield the sum of each word"""
        lst = list(values)
        yield [key], sum(lst)    


if __name__ == '__main__':
    WordCount.run()
