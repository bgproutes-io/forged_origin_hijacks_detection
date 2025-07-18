import sys
import networkx as nx
import csv 
import pandas as pd
from sklearn.preprocessing import Normalizer

class FacilityFeaturesComputation:
    def __init__(self, topo_file, facility_file):
        self.topo_file = topo_file
        self.facility_file = facility_file

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

        self.node_to_facilities = {}
        self.node_to_cities = {}
        self.node_to_countries = {}
        self.mapping_facilities = {}
        self.mapping_cities = {}
        self.mapping_countries = {}

        # Process the facility file and collect info about facilities, countries and cities for every AS.
        with open(self.facility_file, 'r') as fd:
            
            csv_reader = csv.reader(fd, delimiter=' ')            
            for row in csv_reader:
                fac_list = ' '.join(row[1:])
                if len(fac_list) == 0:
                    continue

                node = int(row[0])
                for fac in fac_list.split('),('):
                    # Collect information every node (facilities, countries and cities).
                    fac_id = int(fac.split(',')[0].replace('(', '').replace(')', ''))
                    country = fac.split(',')[2]
                    city = fac.split(',')[3].replace('(', '').replace(')', '')

                    # Adding the facility ID to the corresponding node.
                    if node not in self.node_to_facilities:
                        self.node_to_facilities[node] = set()
                    self.node_to_facilities[node].add(fac_id)

                    if node not in self.node_to_cities:
                        self.node_to_cities[node] = set()
                    self.node_to_cities[node].add(city)

                    if node not in self.node_to_countries:
                        self.node_to_countries[node] = set()
                    self.node_to_countries[node].add(country)

                    # Creating the mappings.
                    if fac_id not in self.mapping_facilities:
                        self.mapping_facilities[fac_id] = len(self.mapping_facilities)
                
                    if city not in self.mapping_cities:
                        self.mapping_cities[city] = len(self.mapping_cities)

                    if country not in self.mapping_countries:
                        self.mapping_countries[country] = len(self.mapping_countries)


    def construct_features_node(self, node, features_node, features_mapping, min_features_nb=1):
        if not self.topo.has_node(node):
            print ('Error construct_features_nodes: node {} not in the graph'.format(node), file=sys.stderr)
            return 

        # Initialize the feature vector for that node.
        fval = [0] * (len(features_mapping))

        if min_features_nb <= 1:
            for cur_node in self.topo.neighbors(node):
                if cur_node in features_node:
                    for fac_id in features_node[cur_node]:
                        fval[features_mapping[fac_id]] += 1

        else: # In case we go further than 1-hop away.
            # Filling the country array for that node.
            current_nodes = set(list(self.topo.neighbors(node)))
            next_nodes = set()
            passed_nodes = set(list(self.topo.neighbors(node)))
            passed_nodes.add(node)

            # Make sure to fill the array with at least min_features_nb countries.
            while sum(fval) < min_features_nb:
                for cur_node in current_nodes:
                    if cur_node in features_node:
                        for fac_id in features_node[cur_node]:
                            # Update the value at the corresponding index (recall fac IDs start a 1).
                            fval[features_mapping[fac_id]] += 1

                    for ngh in self.topo.neighbors(cur_node):
                        if ngh not in passed_nodes:
                            next_nodes.add(ngh)
                            passed_nodes.add(ngh)

                current_nodes = next_nodes
                next_nodes = set()

        return fval

    def construct_features(self, features_node, features_mapping, outfile, normalized: bool=True):
        iter_n = 0
        cur_fval = []
        findex = []

        # Compute the country feature for every node.
        for node in self.topo.nodes():
            cur_fval.append(self.construct_features_node(node, features_node, features_mapping))
            findex.append(node)
           
        features = pd.DataFrame(cur_fval, \
            columns=list(range(0, len(features_mapping))), \
            index=findex)

        if outfile is not None:
            if normalized:
                # Normalize the data.
                transformer = Normalizer().fit(features)  # fit does nothing.
                tmp = transformer.transform(features)
                features_normalized = pd.DataFrame(tmp, \
                    columns=list(range(0, len(features_mapping))), \
                    index=features.index)

                features_normalized.to_pickle(outfile)
            else:
                features.to_pickle(outfile)
            

if __name__ == "__main__":

    cfc = FacilityFeaturesComputation( \
        '2022-01-01_full.txt', \
        '2022-01-01_facility.txt')

    cfc.construct_features(cfc.node_to_facilities, cfc.mapping_facilities, outfile="features_facility.pkl")
    cfc.construct_features(cfc.node_to_cities, cfc.mapping_cities, outfile="features_facility_cities.pkl")
    cfc.construct_features(cfc.node_to_countries, cfc.mapping_countries, outfile="features_facility_countries.pkl")
    # cfc.construct_features_node(1782)
    # print (cfc.features)

#    2914 -> 1491
