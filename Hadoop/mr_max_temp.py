from mrjob.job import MRJob

class MRMaxTemp(MRJob):

    def to_fahrenheiht(self,tenth_of_celsius):
        celsius = float(tenth_of_celsius)/ 10
        fahrenheiht = celsius * 1.8 + 32
        return fahrenheiht

    def mapper(self,key,line):
        (location,date,temp_type,data,x,y,z,w) = line.split(',')
        if temp_type == 'TMAX':
            temp = self.to_fahrenheiht(data)
            yield location, temp

    def reducer(self,location,temps):
        yield location, max(temps)


if __name__ == '__main__':
    MRMaxTemp.run()

