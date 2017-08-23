from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostRatedMovie(MRJob):
    
    # tell MRJob we have an additional command line arg
    # when we run this script
    def configure_options(self):
        super(MRMostRatedMovie, self).configure_options()
        self.add_file_option("--items", help = "Path to u.item")

    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_ratings,
                   reducer_init = self.reducer_init,
                   reducer = self.reducer_count_ratings),
            MRStep(reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (uId, mId, rating, timestamp) = line.split("\t")
        yield mId, 1
    
    # runs before first reducer, to populate data
    # to be used in reducer to output actual movie names
    def reducer_init(self):
        self.movieNames = {}

        with open("u.ITEM", encoding = "ISO-8859-1") as file:
            for line in file:
                fields = line.split("|")
                self.movieNames[fields[0]] = fields[1]
    
    def reducer_count_ratings(self, mId, ratings):
        yield None, (sum(ratings), self.movieNames[mId])
    
    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == "__main__":
    MRMostRatedMovie.run()