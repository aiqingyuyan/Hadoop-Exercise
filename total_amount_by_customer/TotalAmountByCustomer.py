from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTotalAmountByCustomer(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_amount,
                   reducer = self.reducer_sum_amount),
            MRStep(mapper = self.mapper_make_amount_key,
                   reducer = self.reducer_output_total_amount)
        ]

    def mapper_get_amount(self, _, line):
        (customer, item, amount) = line.split(",")
        yield customer, float(amount)
    
    def reducer_sum_amount(self, customer, listOfAmount):
        yield customer, sum(listOfAmount)
    
    def mapper_make_amount_key(self, customer, totalAmount):
        yield "%04.02f" % totalAmount, customer
    
    def reducer_output_total_amount(self, totalAmount, customers):
        for customer in customers:
            yield customer, totalAmount        

if __name__ == "__main__":
    MRTotalAmountByCustomer.run()