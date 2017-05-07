from mrjob.job import MRJob
from mrjob.step import MRStep


class MRMostPopularSuperhero(MRJob):

    def configure_options(self):
        super().configure_options()
        self.add_file_option('--names',help='Path to Marvel-Names.txt')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_num_of_friends,reducer=self.reducer_sum_num_of_friends),
            MRStep(reducer_init=self.reducer_init,reducer=self.reducer_most_popular)
        ]

    def mapper_get_num_of_friends(self,key,line):
        data = line.split()
        hero_id = data[0]
        num_of_friends = len(data)-1
        yield hero_id, num_of_friends

    def reducer_sum_num_of_friends(self,hero_id,num_of_friends):
        yield None, (sum(num_of_friends),hero_id)

    def reducer_init(self):
        self.superhero_names = {}

        with open('Marvel-Names.txt',encoding='ISO-8859-1') as f:
            for line in f:
                data = line.split('"')
                self.superhero_names[int(data[0])] = data[1]

    def reducer_most_popular(self,key,num_of_friends):
        most_popular = max(num_of_friends)
        yield self.superhero_names[int(most_popular[1])], most_popular[0]

if __name__ == '__main__':
    MRMostPopularSuperhero.run()

