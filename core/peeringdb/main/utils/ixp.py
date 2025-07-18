import sys
import networkx as nx
import csv 
import pandas as pd
from sklearn.preprocessing import Normalizer

class IXPFeaturesComputation:
    def __init__(self, topo_file, ixp_file):
        self.topo_file = topo_file
        self.ixp_file = ixp_file

        # Build the AS-level topology.
        self.topo = nx.Graph()

        with open(topo_file, 'r') as fd:
            for line in fd.readlines():
                if line.startswith('#'):
                    continue

                linetab = line.rstrip('\n').split(' ')
                as1 = int(linetab[0])
                as2 = int(linetab[1])

                self.topo.add_edge(as1, as2)

        self.node_to_ixp = {}
        self.mapping_ixp = {}

        # Process the facility file and collect info about facilities, countries and cities for every AS.
        with open(self.ixp_file, 'r') as fd:
            
            csv_reader = csv.reader(fd, delimiter=' ')            
            for row in csv_reader:
                ixp_list = ' '.join(row[1:])
                if len(ixp_list) == 0:
                    continue

                node = int(row[0])
                for ixp in ixp_list.split('),('):
                    # Collect information every node (facilities, countries and cities).
                    ixp_id = int(ixp.split(',')[0].replace('(', '').replace(')', ''))
                    name = ixp.split(',')[1].replace('(', '').replace(')', '')

                    # Adding the facility ID to the corresponding node.
                    if node not in self.node_to_ixp:
                        self.node_to_ixp[node] = set()
                    self.node_to_ixp[node].add(ixp_id)

                    # Creating the mappings.
                    if ixp_id not in self.mapping_ixp:
                        self.mapping_ixp[ixp_id] = len(self.mapping_ixp)
                

    def construct_features_node_neighborhood(self, node, min_features_nb=1):
        if not self.topo.has_node(node):
            print ('Error construct_features_nodes: node {} not in the graph'.format(node), file=sys.stderr)
            return 

        # Initialize the feature vector for that node.
        fval = [0] * (len(self.mapping_ixp))

        # Filling the country array for that node.
        if min_features_nb <= 1:
            for cur_node in self.topo.neighbors(node):
                if cur_node in self.node_to_ixp:
                    for ixp_id in self.node_to_ixp[cur_node]:
                        fval[self.mapping_ixp[ixp_id]] += 1

        else:# In case we go further than 1-hop away.
            current_nodes = set(list(self.topo.neighbors(node)))
            next_nodes = set()
            passed_nodes = set(list(self.topo.neighbors(node)))
            passed_nodes.add(node)

            # Make sure to fill the array with at least min_features_nb countries.
            while sum(fval) < min_features_nb:
                for cur_node in current_nodes:
                    if cur_node in self.node_to_ixp:
                        for ixp_id in self.node_to_ixp[cur_node]:
                            # Update the value at the corresponding index (recall fac IDs start a 1).
                            fval[self.mapping_ixp[ixp_id]] += 1

                    for ngh in self.topo.neighbors(cur_node):
                        if ngh not in passed_nodes:
                            next_nodes.add(ngh)
                            passed_nodes.add(ngh)

                current_nodes = next_nodes
                next_nodes = set()

        return fval

    def construct_features(self, outfile, normalized: bool=True):
        iter_n = 0
        cur_fval = []
        findex = []

        # Compute the country feature for every node.
        for node in self.topo.nodes():
            cur_fval.append(self.construct_features_node_neighborhood(node))
            findex.append(node)
      
        # Build the dataframe.
        features = pd.DataFrame(cur_fval, \
            columns=list(range(0, len(self.mapping_ixp))), \
            index=findex)

        if outfile is not None:
            if normalized:
                # Normalize the data.
                transformer = Normalizer().fit(features)  # fit does nothing.
                tmp = transformer.transform(features)
                features_normalized = pd.DataFrame(tmp, \
                    columns=list(range(0, len(self.mapping_ixp))), \
                    index=features.index)

                features_normalized.to_pickle(outfile)
            else:
                features.to_pickle(outfile)

if __name__ == "__main__":

    cfc = IXPFeaturesComputation( \
        '/home/holterbach/data/merged_topo/2021-11-30:00-00-00_merged.txt', \
        '/home/holterbach/data/peeringdb/as_ixp_caida.txt')

    cfc.construct_features(outfile="features_ixp.pkl")
    # cfc.construct_features_node(1782)
    # print (cfc.features)

#    2914 -> 1491
