import sys
import networkx as nx
import csv 
import pandas as pd
from sklearn.preprocessing import Normalizer

class CountryFeaturesComputation:
    def __init__(self, topo_file, country_file):
        self.topo_file = topo_file
        self.country_file = country_file

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

        # Build the variables pertained to the country information.
        self.header = []
        self.node_to_country = {}

        with open(self.country_file, 'r') as fd:
            csv_reader = csv.reader(fd, delimiter=' ')

            countries_id = {}
            for row in csv_reader:
                if row[1] not in countries_id:
                    countries_id[row[1]] = len(countries_id)
                    self.header.append(row[1])
                self.node_to_country[int(row[0])] = row[1], countries_id[row[1]]

        self.nb_countries = len(countries_id)

    def construct_features_node(self, node, min_features_nb=1):
        if not self.topo.has_node(node):
            print ('Error construct_features_nodes: node {} not in the graph'.format(node), file=sys.stderr)
            return 

        fval = [0] * self.nb_countries

        # Filling the country array for that node.
        if min_features_nb <= 1:
            for cur_node in self.topo.neighbors(node):
                if cur_node in self.node_to_country:
                    country, position = self.node_to_country[cur_node]
                    fval[position] += 1

        else: # In case we go further than 1-hop away.
            current_nodes = set(list(self.topo.neighbors(node)))
            next_nodes = set()
            passed_nodes = set(list(self.topo.neighbors(node)))
            passed_nodes.add(node)

            # Make sure to fill the array with at least min_features_nb countries.
            while sum(fval) < min_features_nb:
                for cur_node in current_nodes:
                    if cur_node in self.node_to_country:
                        country, position = self.node_to_country[cur_node]
                        fval[position] += 1

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
            cur_fval.append(self.construct_features_node(node))
            findex.append(node)

        # Build the dataframe.
        features = pd.DataFrame(cur_fval, \
            columns=self.header, \
            index=findex)

        if outfile is not None:
            if normalized:
                # Normalize the data.
                transformer = Normalizer().fit(features)  # fit does nothing.
                tmp = transformer.transform(features)
                features_normalized = pd.DataFrame(tmp, \
                    columns=self.header, \
                    index=features.index)

                features_normalized.to_pickle(outfile)
            else:
                features.to_pickle(outfile)

if __name__ == "__main__":

    cfc = CountryFeaturesComputation( \
        '2022-01-01_full.txt', \
        '2022-01-01_country.txt')

    cfc.construct_features(outfile="features.pkl")
    # cfc.construct_features_node(60341)