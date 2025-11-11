# libraries
from mrjob.job import MRJob
from mrjob.step import MRStep


# defining the class
class InvertedIndex(MRJob):
    def mapper(self, key, line):
        """Mapper method"""

        # filename = self.get_input_file_name().split('/')[-1]

        doc, *words = line.strip().split()

        for word in words:
            yield word.lower(), doc
             

    def reducer(self, key, values):
        """reducer method"""

        unique_docs = set(values)
        docs = list(unique_docs)

        yield key, docs


if __name__ == '__main__':
    InvertedIndex.run()
