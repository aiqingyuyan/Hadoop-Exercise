# technique: item-based collaborative filtering
# - find every pair of movies that were watched by the same person
# - measure the similarity of their ratings across all users who watched both
# - sort by movie, then by similarity strength
# one way of of doing movie similarity

# improvement of this algorithm
# - discard bad ratings - only recommend good movies
# - try different similarity metrics (Pearson Correlation Coefficient, 
#   Jaccard Coefficient, Conditional Probability)
# - adjust the thresholds for minimum co-raters or minimum score
# - invent a new similarity metric that takes the number of co0raters into account
# - use genre infomation in u.items to boost scores from movies in the same genre

from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from itertools import combinations   # used to compute permutation pairs

class MovieSimilarities(MRJob):
    
    def load_movie_names(self):
        self.movie_names = {}

        with open("u.item", encoding = 'ascii', errors = 'ignore') as file:
            for line in file:
                fields = line.split("|")
                self.movie_names[int(fields[0])] = fields[1]

    def configure_options(self):
        super(MovieSimilarities, self).configure_options()
        self.add_file_option("--items", help = "Path to u.item")
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_parse_input,
                   reducer = self.reducer_ratings_by_user),
            MRStep(mapper = self.mapper_create_item_pairs,
                   reducer = self.reducer_compute_similarity),
            MRStep(mapper = self.mapper_sort_similarities,
                   mapper_init = self.load_movie_names,
                   reducer = self.reducer_output_similarities)
        ]

    def mapper_parse_input(self, _, line):
        """
        Outputs user_id -> (movie_id, rating)
        """

        (user_id, movie_id, rating, timestamp) = line.split('\t')
        yield user_id, (movie_id, float(rating))
    
    def reducer_ratings_by_user(self, user_id, movie_rating_pairs):
        """
        Group (movie, rating) pairs by user_id
        Output user_id -> list of (movie_id, rating)
        """

        ratings = []
        for movie_id, rating in movie_rating_pairs:
            ratings.append((movie_id, rating))
        
        yield user_id, ratings
    
    def mapper_create_item_pairs(self, user_id, movie_rating_pairs):
        """
        Find every pair of movies each user has seen, and emit 
        each pair with its associated ratings
        Output (movie_x, movie_y) -> (rating_x, rating_y)
        """

        # "combinations" finds every possible pair from the list of movies
        # this user viewed
        for movie_rating_1, movie_rating_2 in combinations(movie_rating_pairs, 2):
            movie_id_1 = movie_rating_1[0]
            rating_1 = movie_rating_1[1]
            movie_id_2 = movie_rating_2[0]
            rating_2 = movie_rating_2[1]

            # produce both orders so sims are bi-directional
            yield (movie_id_1, movie_id_2), (rating_1, rating_2)
            yield (movie_id_2, movie_id_1), (rating_2, rating_1)
    
    def cosine_similarity(self, rating_pairs):
        # compute cosine similarity metric between two rating vectors
        num_pairs = 0
        sum_xx = sum_yy = sum_xy = 0

        for rating_x, rating_y in rating_pairs:
            sum_xx += rating_x * rating_x
            sum_yy += rating_y * rating_y
            sum_xy += rating_x * rating_y
            num_pairs += 1
        
        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if denominator:
            score = numerator / (float(denominator))
        
        return (score, num_pairs)
    
    def reducer_compute_similarity(self, movie_pair, rating_pairs):
        """
        Compute the similarity score between the ratings vectors
        for each movie pair viewed by multiple people
        (rating_pairs is a list of (r_x, r_y), think all r_x from rating_pairs
        construct X vector, all r_y construct Y vector)
        Output movie_pair -> score, number of co-ratings
        """

        score, num_pairs = self.cosine_similarity(rating_pairs)

        # enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if num_pairs > 10 and score > 0.95:
            yield movie_pair, (score, num_pairs)

    
    def mapper_sort_similarities(self, movie_pair, scores):
        """
        Shuffle thing around so the key is (movie1, score)
        so we have meaningfully sorted results
        """

        score, n = scores
        movie1, movie2 = movie_pair

        yield (self.movie_names[int(movie1)], score), \
              (self.movie_names[int(movie2)], n)
    
    def reducer_output_similarities(self, movie_score, similar_n):
        """
        Output the result
        movie -> similar movie, score, number of co-ratings
        """

        movie1, score = movie_score
        
        for movie2, n in similar_n:
            yield movie1, (movie2, score, n)


if __name__ == "__main__":
    MovieSimilarities.run()