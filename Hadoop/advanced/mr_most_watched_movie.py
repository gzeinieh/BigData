from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostWatchedMovie(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie,reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_max)
        ]

    def mapper_get_movie(self,key,line):
        (user,movie,rating,time) = line.split('\t')
        yield movie, 1

    def reducer_sum(self,movie,occurences):
        yield None, (sum(occurences),movie)

    def reducer_max(self,key,occurences_movie):
        yield max(occurences_movie)


if __name__ == '__main__':
    MRMostWatchedMovie.run()