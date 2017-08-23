from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

# Multi stage map reduce
# to sort output by key in order to get most frequent word
class MRWordFrequency(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words,
                   reducer = self.reducer_count_word),
            MRStep(mapper = self.mapper_make_counts_key,
                   reducer = self.reducer_output_words)
        ]

    def mapper_get_words(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1
    
    def reducer_count_word(self, word, frequencies):
        yield word, sum(frequencies)
    
    # key step
    # The default key type is String in MRJob input 
    # that is why we rely on natural string sort order
    def mapper_make_counts_key(self, word, count):
        yield "%04d" % int(count), word
    
    def reducer_output_words(self, count, words):
        for word in words:
            yield count, word

if __name__ == "__main__":
    MRWordFrequency.run()
