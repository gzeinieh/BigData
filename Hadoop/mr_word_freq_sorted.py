from mrjob.job import MRJob
from mrjob.step import MRStep
import re

word_regex = re.compile(r"[\w']+")

class MRWordFreq(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_make_count_key,reducer=self.reducer_output_words)
        ]

    def mapper_get_words(self,key,line):
        words = word_regex.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer_count_words(self,word,freq):
        yield word, sum(freq)

    def mapper_make_count_key(self,word,count):
        yield '%04d'%int(count), word

    def reducer_output_words(self,count,words):
        for word in words:
            yield count, word


if __name__ == '__main__':
    MRWordFreq.run()