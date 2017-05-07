from mrjob.job import MRJob
from mrjob.step import MRStep

class MRMostWatchedMovie(MRJob):

    def configure_options(self):
        super().configure_options()
        self.add_file_option('--items',help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_movie,reducer=self.reducer_sum),
            MRStep(reducer=self.reducer_max,reducer_init=self.reducer_init)
        ]

    def mapper_get_movie(self,key,line):
        (user,movie,rating,time) = line.split('\t')
        yield movie, 1


    def reducer_sum(self,movie,occurences):
        yield None, (sum(occurences),movie)

    def reducer_init(self):
        self.movies_names = {}

        with open('u.ITEM',encoding='ISO-8859-1') as f:
            for line in f:
                data = line.split('|')
                self.movies_names[data[0]] = data[1]

    def reducer_max(self,key,occurences_movie):
        most_watched = max(occurences_movie)
        yield most_watched[0],self.movies_names[most_watched[1]]


if __name__ == '__main__':
    MRMostWatchedMovie.run()