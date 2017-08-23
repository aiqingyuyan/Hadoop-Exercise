from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperhero(MRJob):
    
    def configure_options(self):
        super(MostPopularSuperhero, self).configure_options()
        self.add_file_option("--names", help = "Path to Marvel-names.txt")
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_count_friends_per_line,
                   reducer = self.reducer_combine_friends),
            MRStep(mapper_init = self.load_name_into_dictionary,
                   mapper = self.mapper_prep_for_sort,
                   reducer = self.reducer_find_max_friends)
        ]
    
    def mapper_count_friends_per_line(self, _, line):
        fields = line.split()
        hero_id = fields[0]
        num_friends = len(fields) - 1
        yield int(hero_id), int(num_friends)
    
    def reducer_combine_friends(self, hero_id, friends):
        yield hero_id, sum(friends)
    
    def load_name_into_dictionary(self):
        self.hero_names = {}

        with open("Marvel-names.txt", encoding = "ISO-8859-1") as file:
            for line in file:
                fields = line.split('"')
                self.hero_names[int(fields[0])] = fields[1]
    
    def mapper_prep_for_sort(self, hero_id, friendCounts):
        hero_name = self.hero_names[hero_id]
        yield None, (friendCounts, hero_name)
    
    def reducer_find_max_friends(self, key, values):
        yield max(values)

if __name__ == "__main__":
    MostPopularSuperhero.run()