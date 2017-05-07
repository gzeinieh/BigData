from mrjob.job import MRJob
import re

word_regex = re.compile(r"[\w']+")

class MRWordFreq(MRJob):

    def mapper(self,key,line):
        words = word_regex.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self,word,freq):
        yield word, sum(freq)


if __name__ == '__main__':
    MRWordFreq.run()