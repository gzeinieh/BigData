from mrjob.job import MRJob

class MRMaxTemp(MRJob):

    def to_fahrenheight(self,tenth_of_celsius):
        celsius = float(tenth_of_celsius)/ 10
        fahrenheight = celsius * 1.8 + 32
        return fahrenheight

    def mapper(self,key,line):
        (location,date,temp_type,data,x,y,z,w) = line.split(',')
        if temp_type == 'TMAX':
            temp = self.to_fahrenheight(data)
            yield location, temp

    def reducer(self,location,temps):
        yield location, max(temps)


if __name__ == '__main__':
    MRMaxTemp.run()