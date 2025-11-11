# importing the relevant libraries
from mrjob.step import MRStep
from mrjob.job import MRJob

# defining the class
class FrequentWord(MRJob):
    def mapper(self, _, line):
        """The mapper function"""

        words = line.split()

        for word in words:
            yield word.lower(), 1
    
    def reducer(self, key, values):

        freq = list(values)

        yield None, (key, sum(freq))

    
    def frequent_word_reducer(self, _, pair):
        max_word = None
        max_count = 0

        for (word, count) in pair:
            if count > max_count:
                max_count = count
                max_word = word
        yield max_word, max_count


    def steps(self):
        return [MRStep(mapper=self.mapper),
                MRStep(reducer=self.reducer),
                MRStep(reducer=self.frequent_word_reducer)]


if __name__ == '__main__':
    FrequentWord.run()