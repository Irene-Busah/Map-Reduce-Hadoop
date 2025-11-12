# libraries
from mrjob.job import MRJob
from mrjob.step import MRStep
import math


# defining the class
class TFIDF(MRJob):

    # defining the mapper method
    def mapper(self, key, line):
        """
        Mapper
        
        Sample Input
        - doc1: cat sat on the mat
        """

        docs, *words = line.strip().split()

        for word in words:
            yield (word, docs), 1


    # defining the reducer method
    def reducer(self, key, values):
        """Reducer"""

        word, doc = key

        sum_of_values = list(values)

        yield word, (doc, sum(sum_of_values))

    
    def tf_reducer(self, key, values):
        """Second Reducer"""

        values = list(values)  # Convert generator to list for reuse

        # Extract all documents this word appears in
        doc_names = [doc for (doc, _) in values]
        num_docs_with_word = len(set(doc_names))  # df (document frequency)

        # 🔹 Compute total number of unique documents across all input pairs
        # Since MRJob reducer sees only one word at a time,
        # total_docs is the total number of distinct docs that appear in the input values.
        # If TFs come from all docs, we can infer this directly:
        all_docs_seen = set()
        for doc, _ in values:
            all_docs_seen.add(doc)
        total_docs = len(all_docs_seen)  # total_docs = number of distinct docs seen

        # Compute IDF and TF-IDF
        idf = math.log((total_docs + 1) / (1 + num_docs_with_word)) + 1  # +1 to avoid div-by-zero
        for doc, tf in values:
            tfidf = tf * idf
            yield (key, doc), round(tfidf, 4)


    def steps(self):
        return [
            MRStep(mapper=self.mapper),
            MRStep(reducer=self.reducer),
            MRStep(reducer=self.tf_reducer)
        ]


    

if __name__ == '__main__':
    TFIDF.run()
