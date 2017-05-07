from mrjob.job import MRJob


class MRRatingCounter(MRJob):

    def mapper(self,key,line):
        (userID,movieID,rating,timestamp) = line.split('\t')
        yield userID, int(rating)

    def reducer(self,userID,rating):
        total = 0
        num = 0
        for r in rating:
            total += r
            num +=1
        yield userID, total/num


if __name__ == '__main__':
    MRRatingCounter.run()
