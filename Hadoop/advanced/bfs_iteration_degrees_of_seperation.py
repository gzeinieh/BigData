from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node():

    def __init__(self):
        self.hero_id = None
        self.connections = None
        self.distance = None
        self.color = None

    def from_line(self,line):
        data = line.split('|')
        if len(data) ==4:
            self.hero_id = data[0]
            self.connections = data[1].split(',')
            self.distance = int(data[2])
            self.color = data[3]

    def to_line(self):
        edges = ",".join(self.connections)
        return "|".join((self.hero_id,edges,str(self.distance),self.color))

class MRBfsIterationDegreeOfSeperation(MRJob):

    def configure_options(self):
        pass

    def steps(self):
        pass

    def mapper(self,key,line):
        pass

    def reducer(self):
        pass

if __name__ == "__main__":
    MRBfsIterationDegreeOfSeperation.run()





