# To run the job:
# python MovieRecommendationCosine.py --items=ml-1m/movies.dat ml-1m/ratings.dat > moviesLarge.txt
# Input :- ml-1m folder
# Output :- moviesLarge.txt

# Expected Runtime 2 Hours
# Future Advancement: Trying to run the script on Amazon Elastic MapReduce(EMR) for time saving.

from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

class MovieCosine(MRJob):

    def configure_options(self):
        super(MovieCosine, self).configure_options()
        self.add_file_option('--items', help='Path to movies.dat')

    def load_movie_names(self):
        self.movieNames = {}

        file = open("movies.dat")
        for line in file:
            names = line.split('::')
            if (names[0] != 'movieId'):
                self.movieNames[int(names[0])] = names[1].decode('utf-8', 'ignore')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_similarities)]

    def mapper_parse_input(self, key, line):
        # Outputs userID => (movieID, rating)
        (userID, movieID, rating, timestamp) = line.split('::')
        if (userID != 'userId'): 
            yield  userID, (movieID, float(rating))

    def reducer_ratings_by_user(self, user_id, itemRatings):
        #Group (item, rating) pairs by userID

        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))

        yield user_id, ratings

    def mapper_create_item_pairs(self, user_id, IDRatings):
        for IDRating1, IDRating2 in combinations(IDRatings, 2): # Generating combinations of the rating list for a unique user
            movieID1 = IDRating1[0]
            rating1 = IDRating1[1]
            movieID2 = IDRating2[0]
            rating2 = IDRating2[1]
            # Yielding Each Pair of MovieId and Rating
            yield (movieID1, movieID2), (rating1, rating2) 
            yield (movieID2, movieID1), (rating2, rating1)


    def cosine_similarity(self, ratingPairs):
    	# This function is used for calculating the similarity score for each user.

        numPairs = 0 # Initialising Number of pairs in the list
        sum_xx = sum_yy = sum_xy = 0
        
        for ratingX, ratingY in ratingPairs: 
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarity(self, moviePair, ratingPairs):
        
        score, numPairs = self.cosine_similarity(ratingPairs)
        # If the number of pairs and score is upto the mark there is a possible recommendation for the suggested movie.

        if (numPairs > 10 and score > 0.95): # Adjust the numPairs and score for accuracy 
            yield moviePair, (score, numPairs)

    def mapper_sort_similarities(self, moviePair, scores):
        score, n = scores
        movie1, movie2 = moviePair

        yield (self.movieNames[int(movie1)], score), \
            (self.movieNames[int(movie2)], n)

    def reducer_output_similarities(self, movieScore, similarN):
        movie1, score = movieScore
        for movie2, n in similarN:
            yield movie1, (movie2, score, n)


if __name__ == '__main__':
    MovieCosine.run()
