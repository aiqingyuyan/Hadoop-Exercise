from mrjob.job import MRJob
from mrjob.protocol import RawValueProtocol

class Node:

    def __init__(self):
        self.character_id = ''
        self.connections = []
        self.distance = 9999
        self.color = "WHITE"
    
    # Format is ID|EDGES|DISTANCE|COLOR
    def from_line(self, line):
        fields = line.split('|')
        if len(fields) == 4:
            self.character_id = fields[0]
            self.connections = fields[1].split(',')
            self.distance = int(fields[2])
            self.color = fields[3]
    
    def get_line(self):
        connections = ','.join(self.connections)
        return '|'.join( (self.character_id, connections, str(self.distance), self.color) )

class MRBFSIteration(MRJob):

    # by default, the output will be josn format
    # here since we will be doing multiple iteration
    # so we don't want to mess the output format, and use RawValueProtocol
    INPUT_PROTOCOL = RawValueProtocol
    OUTPUT_PROTOCOL = RawValueProtocol

    def configure_options(self):
        super(MRBFSIteration, self).configure_options()
        # make sure information being passed through everybody that needs it
        self.add_passthrough_option(
            "--target", help = "ID of character we are searching for")
    
    def mapper(self, _, line):
        node = Node()
        node.from_line(line)
        # if this node needs to be explored
        if node.color == "GREY":
            # start a BFS search
            for connection in node.connections:
                vnode = Node()
                vnode.character_id = connection
                # distance from initial node (node with 'GREY' in BFS-iteration-0) to curent node
                # start from second iteration, this step will change
                # the distance value of already fully explored node
                vnode.distance = int(node.distance) + 1
                # start from second iteration, this step will color 
                # black node back to grey node
                vnode.color = "GREY"
                # so, in reducer step, there are if conditions to
                # change color back to black, and distance back to shortest

                if self.options.target == connection:
                    counter_name = ("Target ID " + connection + 
                        " was hit with distance " + str(vnode.distance))
                    self.increment_counter("Degrees of Separation", counter_name, 1)
                
                yield connection, vnode.get_line()
            
            # this node have been fully explored, so color it BLACK
            node.color = "BLACK"
        
        yield node.character_id, node.get_line()
    
    def reducer(self, key, values):
        edges = []
        distance = 9999
        color = "WHITE"

        for value in values:
            node = Node()
            node.from_line(value)

            if len(node.connections) > 0:
                edges.extend(node.connections)
            
            if node.distance < distance:
                distance = node.distance
            
            if node.color == "BLACK":
                color = "BLACK"
            
            if node.color == "GREY" and color == "WHITE":
                color = "GREY"
        
        node = Node()
        node.character_id = key
        node.distance = distance
        node.color = color
        node.connections = edges

        yield key, node.get_line()


if __name__ == "__main__":
    MRBFSIteration.run()