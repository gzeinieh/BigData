from mrjob.job import MRJob


class MRMoviesWatchedByUser2(MRJob):

    def mapper(self,key,line):
        (user_id,movie_id,rating,timestamp) = line.split('\t')
        yield user_id, 1

    def reducer(self,user_id,occurrences):
        yield user_id, sum(occurrences)


if __name__ == '__main__':
    MRMoviesWatchedByUser2.run()
