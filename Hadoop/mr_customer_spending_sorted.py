from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCustomerSpending(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_order,reducer=self.reducer_sum_orders),
            MRStep(mapper=self.mapper_make_orders_key,reducer=self.reducer_output_results)

        ]

    def mapper_get_order(self,key,line):
        (customer,item,order) = line.split(',')
        yield customer, float(order)


    def reducer_sum_orders(self,customer,orders):
        yield customer, sum(orders)

    def mapper_make_orders_key(self,customer,total_orders):
        yield total_orders, customer

    def reducer_output_results(self,total_orders,customers):
        for customer in customers:
            yield total_orders, customer


if __name__ == '__main__':
    MRCustomerSpending.run()