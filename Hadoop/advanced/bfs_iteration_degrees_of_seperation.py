from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node():

    def __init__(self):
        self.hero_id = ''
        self.connections = []
        self.distance = 9999
        self.color = 'WHITE'

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

    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super().configure_options()
        self.add_passthrough_option('--target',help="ID of character we are searching for")

    def mapper(self,key,line):
        node = Node()
        node.from_line(line)
        # if this node needs to be expanded
        if node.color == 'GRAY':
            for connection in node.connections:
                vnode = Node()
                vnode.hero_id = connection
                vnode.distance = int(node.distance) + 1
                vnode.color = 'GRAY'
                if self.options.target == connection:
                    counter_name = ("Target ID " + connection +
                                   " was hit with distance " + str(vnode.distance))
                    self.increment_counter('Degrees of Separation',
                                           counter_name, 1)
                yield connection, vnode.to_line()

            # mark this node black
            node.color = 'BLACK'
        # add the input node
        yield node.hero_id, node.to_line()


    def reducer(self,hero_id,values):
        edges = []
        distance = 9999
        color = 'WHITE'

        for value in values:

            node = Node()
            node.from_line(value)

            if len(node.connections) > 0:
                edges.extend(node.connections)

            if node.color == 'BLACK':
                color = node.color

            if node.color == 'GRAY' and color =='WHITE':
                color = node.color

            if node.distance < distance:
                distance = node.distance

        node = Node()
        node.hero_id = hero_id
        node.connections = edges
        node.distance = distance
        node.color = color

        yield hero_id, node.to_line()

if __name__ == "__main__":
    MRBfsIterationDegreeOfSeperation.run()





