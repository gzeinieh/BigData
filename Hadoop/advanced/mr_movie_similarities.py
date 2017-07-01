# To run on a single EMR node:
# !python mr_movie_similarities.py -r emr --items=ml-100k/u.item ml-100k/u.data

# To run on 4 EMR nodes:
#!python mr_movie_similarities.py -r emr --num-ec2-instances=4 --items=ml-100k/u.item ml-100k/u.data

# Troubleshooting EMR jobs (subsitute your job ID):
# !python -m mrjob.tools.emr.fetch_logs --find-failure j-1NXMMBNEQHAFT

from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from itertools import combinations

class MRMovieSimilaroties(MRJob):

    def configure_options(self):
        super().configure_options()
        self.add_file_option('--items',help='Path to u.item')

    def load_movie_names(self):
        # load database of movie names
        self.movie_names = {}

        with open("u.item",encoding='ascii',errors='ignore') as f:
            for line in f:
                data = line.split('|')
                self.movie_names[int(data[0])] = data[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,reducer=self.reducer_calculate_similarity),
            MRStep(mapper=self.mapper_sort_similarities,mapper_init=self.load_movie_names,reducer=self.reducer_output_results)
        ]

    def mapper_parse_input(self,key,line):
        # Outputs user_id => (movie_id, rating)
        user_id, movie_id, rating, time_stamp = line.split('\t')
        yield user_id, (movie_id,float(rating))

    def reducer_ratings_by_user(self,user_id,movie_rating):
        # Group (movie, rating) pairs by user_id
        ratings = []
        for movie, rating in movie_rating:
            ratings.append((movie,rating))

        yield user_id, ratings


    def mapper_create_item_pairs(self,user_id,ratings):
        # Find every pair of movies each user has seen, and emit
        # each pair with its associated ratings

        # "combinations" finds every possible pair from the list of movies
        # this user viewed.

        for movie_rating_1, movie_rating_2 in combinations(ratings,2):
            movie_1 = movie_rating_1[0]
            rating_1 = movie_rating_1[1]
            movie_2 = movie_rating_2[0]
            rating_2 = movie_rating_2[1]

            # Produce both orders so sims are bi-directional
            yield (movie_1,movie_2),(rating_1,rating_2)
            yield (movie_2,movie_1),(rating_2,rating_1)

    def cosine_similarity(self,raitng_pairs):
        # Computes the cosine similarity metric between two
        # rating vectors.

        num_pairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for rating_x, rating_y in raitng_pairs:
            sum_xx += rating_x * rating_x
            sum_yy += rating_y * rating_y
            sum_xy += rating_x * rating_y
            num_pairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))


        return (score, num_pairs)


    def reducer_calculate_similarity(self,movie_pair, rating_pairs):

        # Compute the similarity score between the ratings vectors
        # for each movie pair viewed by multiple people

        # Output movie pair => score, number of co-ratings
        score, num_pairs = self.cosine_similarity(rating_pairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        if (num_pairs > 10 and score > 0.95):
            yield movie_pair, (score, num_pairs)

    def mapper_sort_similarities(self,movie_pair,score_n):
        # Shuffle things around so the key is (movie_1, score)
        # so we have meaningfully sorted results.
        movie_1, movie_2 = movie_pair
        score, n = score_n

        yield (self.movie_names[int(movie_1)], score), \
            (self.movie_names[int(movie_2)], n)


    def reducer_output_results(self,movie_score,movie_n):
        # Output the results.
        # Movie => Similar Movie, score, number of co-ratings
        movie_1, score = movie_score
        for movie_2, n in movie_n:
            yield movie_1, (movie_2,score,n)

if __name__ == '__main__':
    MRMovieSimilaroties.run()
