from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostRatedMovie(MRJob):

    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_ratings,
                   reducer = self.reducer_count_ratings),
            MRStep(reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (uId, mId, rating, timestamp) = line.split("\t")
        yield mId, 1
    
    def reducer_count_ratings(self, mId, ratings):
        yield None, (sum(ratings), mId)
    
    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == "__main__":
    MRMostRatedMovie.run()