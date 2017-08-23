from mrjob.job import MRJob

class MRWordFrequency(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1
    
    def reducer(self, word, frequencies):
        yield word, sum(frequencies)

if __name__ == "__main__":
    MRWordFrequency.run()
