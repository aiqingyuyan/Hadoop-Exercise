from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequency(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
    
    # an early local reducer job running in mapper node
    # can reduce data duplication and network traffic
    # should have same args signature with reducer
    def combiner(self, word, frequencies):
        yield word, sum(frequencies)
    
    def reducer(self, word, frequencies):
        yield word, sum(frequencies)

if __name__ == "__main__":
    MRWordFrequency.run()
