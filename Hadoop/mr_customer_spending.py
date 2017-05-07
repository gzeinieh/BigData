from mrjob.job import MRJob

class MRCustomerSpending(MRJob):

    def mapper(self,key,line):
        (customer,item,order) = line.split(',')
        yield customer, float(order)


    def reducer(self,customer,orders):
        yield customer, sum(orders)


if __name__ == '__main__':
    MRCustomerSpending.run()