from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequency(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
    
    def reducer(self, word, frequencies):
        yield word, sum(frequencies)

if __name__ == "__main__":
    MRWordFrequency.run()
