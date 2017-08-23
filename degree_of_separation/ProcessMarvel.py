# Call this with one argument: the character ID you are starting from
# e.g., spider man is 5306, the hulk is 2548

import sys

print("Creating BFS starting input for character" + sys.argv[1])

with open("BFS-iteration-0.txt", "w") as out:
    with open("Marvel-graph.txt") as file:
        for line in file:
            fields = line.split()
            hero_id = fields[0]
            num_connections = len(fields) - 1
            connections = fields[-num_connections:]

            color = "WHITE"
            distance = 9999

            if hero_id == sys.argv[1]:
                color = "GREY"
                distance = 0
            
            if hero_id != '':
                edges = ','.join(connections)
                out_str = '|'.join((hero_id, edges, str(distance), color))

                out.write(out_str)
                out.write('\n')
        
        file.close()

out.close()
