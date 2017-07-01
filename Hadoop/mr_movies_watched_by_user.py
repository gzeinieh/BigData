from mrjob.job import MRJob


class MRMoviesWatchedByUser(MRJob):

    def mapper(self,key,line):
        (user_id,movie_id,rating,timestamp) = line.split('\t')
        yield user_id, movie_id

    def reducer(self,user_id,movies):
        count = 0
        for movie in movies:
            count +=1
        yield user_id, count


if __name__ == '__main__':
    MRMoviesWatchedByUser.run()
