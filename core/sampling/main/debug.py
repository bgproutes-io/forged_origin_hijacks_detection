import utils as ut
import networkx as nx
import sys

def build_graph(infile=None, aspath_file="db/paths/2022-01-01_paths.txt"):
    G = nx.Graph()

    if infile == None:
        nbLine = 0
        with open(aspath_file, "r") as f:
            for line in f:

                nbLine += 1
                if nbLine % 1000000 == 0:
                    print("loading graph : Iter {}".format(nbLine), file=sys.stderr)

                asp = line.strip("\n")
                path = ut.aspath_to_list(asp)

                for i in range(0, len(path) - 1):
                    for u in path[i]:
                        for v in path[i+1]:
                            G.add_edge(u, v)
        
        with open("db/topology/2022-01-01_full_topo.txt", "w") as f:
            for (u,v) in G.edges():
                f.write("{} {}\n".format(u, v))
    
    else:
        with open(infile, "r") as f:
            for line in f:
                u = line.strip("\n").split(" ")[0]
                v = line.strip("\n").split(" ")[1]

                G.add_edge(u, v)

    return G


if __name__ == "__main__":
    build_graph()
    