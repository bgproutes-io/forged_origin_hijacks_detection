import networkx as nx
import sys
import csv
import pandas as pd
import numpy as np
from scipy.spatial import distance
from sklearn.feature_selection import VarianceThreshold

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class CosineDistance:
    def __init__(self, topo_file, data_file, feature_selection_threshold=0):
        self.topo_file = topo_file
        self.data_file = data_file

        # Build the AS-level topology.
        print (self.print_prefix()+'Building the topology.', file=sys.stderr)
        self.topo = nx.Graph()

        with open(topo_file, 'r') as fd:
            for line in fd.readlines():
                if line.startswith('#'):
                    continue

                linetab = line.rstrip('\n').split(' ')
                as1 = int(linetab[0])
                as2 = int(linetab[1])

                self.topo.add_edge(as1, as2)

        print (self.print_prefix()+'Loading the features', file=sys.stderr)
        # Load the pickle object with the features.
        self.features = pd.read_pickle(data_file)
        # Run the features selection.
        # By default, this will remove columns for which all the values are equal to 0. 
        sel = VarianceThreshold(feature_selection_threshold)
        # print ("Before features selection")
        # print (self.features)
        self.features = pd.DataFrame(sel.fit_transform(self.features), index=self.features.index)  
        # print ("After features selection")
        # print (self.features)

    def print_prefix(self):
        return Fore.MAGENTA+Style.BRIGHT+"[cosine.py]: "+Style.NORMAL

    def compute_distance(self, links):
        print (self.print_prefix()+'Computing cosine distance', file=sys.stderr)

        df = pd.DataFrame(columns=['as1', 'as2', 'distance'])

        for as1, as2 in links:
            if as1 in self.features.index and as2 in self.features.index and self.features.loc[as1].sum() > 0 and self.features.loc[as2].sum() > 0:
                df.loc[len(df)] = [as1, as2, distance.cosine(self.features.loc[as1], self.features.loc[as2])]
            else:
                if as1 not in self.features.index:
                    print (self.print_prefix()+self.print_prefix()+'AS {} not in index'.format(as1), file=sys.stderr)
                if as2 not in self.features.index:
                    print (self.print_prefix()+self.print_prefix()+'AS {} not in index'.format(as2), file=sys.stderr)

                df.loc[len(df)] = [as1, as2, -1]

        return df

if __name__ == "__main__":

    # fac threshold = 0.001

    cd = CosineDistance('/home/holterbach/data/merged_topo/2021-11-30:00-00-00_merged.txt', \
        '/home/holterbach/bgp_leaks_hijacks_detection/detection/link_prediction/tmp/peeringdb_features_country.pkl', feature_selection_threshold=0)
    # cd.sample(100, [0, 10, 50, 100, 500, 1000, 1500, 3000, 5000, 100000])
    # cd.load_sample(\
    #     '/home/holterbach/data/features_results/2021-11-29:00-00-00_positive_normal_10000.txt', \
    #     '/home/holterbach/data/features_results/2021-11-29:00-00-00_negative_thresholds_10000.txt')
    # cd.compute_distance(\   
    #     outfile_pos="/home/holterbach/data/features_results/2021-11-29:00-00-00_positive_normal_10000_country.txt", \
    #     outfile_neg="/home/holterbach/data/features_results/2021-11-29:00-00-00_negative_thresholds_10000_country.txt")

    cd.load_sample(\
        '/home/holterbach/data/features_results/positive_links_aspaths_mapped.txt', \
        '/home/holterbach/data/features_results/negative_links_aspaths_mapped.txt')
    cd.compute_distance(\
        outfile_pos="/home/holterbach/data/features_results/alfroy_country.txt", \
        outfile_neg="/home/holterbach/data/features_results/alfroy_country.txt")




