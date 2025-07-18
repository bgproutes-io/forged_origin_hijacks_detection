import sys
import urllib.request
import networkx as nx
import pandas as pd


def get_routeviews_peers():
    url = 'http://www.routeviews.org/peers/peering-status.html'

    peer_list = []

    for line in urllib.request.urlopen(url).read().decode().split('\n'):
        if 'routeviews.org' in line:
            line = ' '.join(line.split())
            meta = line.split('|')[0]
            metatab = meta.split(' ')

            collector = metatab[0].replace('.routeviews.org', '')
            asn = int(metatab[1])
            peer_addr = metatab[2]
            nb_pref = int(metatab[3])

            peer_list.append((collector, peer_addr, asn, nb_pref))

    return peer_list

def get_ris_peers():
    url = 'http://www.ris.ripe.net/peerlist/all.shtml'

    peer_list = []
    cur_collector = None

    for line in urllib.request.urlopen(url).read().decode().split('\n'):
        if '<H2>RRC' in line:
            cur_collector = line.split(' -- ')[0].replace('<H2>', '')

        if line.startswith('<tr><td>Up</td><td><a href="https://stat.ripe.net/'):
            linetab = line.split('<td>')
            asn = int(linetab[2].split('>')[1].replace('</a', '').replace('AS', ''))
            peerip = linetab[4].replace('</td>', '')
            nb_pref_ipv4 = linetab[5].replace('</td>', '')
            nb_pref_ipv6 = linetab[6].replace('</td></tr>', '')
            nb_pref = max(nb_pref_ipv4, nb_pref_ipv6)
            peer_list.append((cur_collector, peerip, asn, nb_pref))

    return peer_list    

def get_vps():
    peers_list_rv = get_routeviews_peers()
    peers_list_ris = get_ris_peers()

    dic_peer = {} # ASN -> vps.
    for collector, peerip, asn, nb_pref in peers_list_rv+peers_list_ris:
        if asn not in dic_peer:
            dic_peer[asn] = set()
        dic_peer[asn].add((collector, asn, peerip))

    return dic_peer

class CountNeighboringVPs:
    def __init__(self, topo_file):
        self.topo = nx.Graph()

        # Load the graph on which the features will be computed.
        with open(topo_file, 'r') as fd:
            for line in fd.readlines():
                if line.startswith('#'):
                    continue

                linetab = line.rstrip('\n').split(' ')
                as1 = int(linetab[0])
                as2 = int(linetab[1])

                self.topo.add_edge(as1, as2)

        self.vps = get_vps()
        self.features = None

    # This function computes the number of BGP VPs that are located in the neighboring ASes of a given AS.
    def number_of_ngh_vps(self, asn):
        vps_set = set()

        if asn not in self.topo.nodes:
            return vps_set

        for ngh in self.topo.neighbors(asn):
            if ngh in self.vps:
                # vps_set = vps_set.union(self.vps[self.mapping[ngh]])
                vps_set.add(ngh)

        return vps_set

    def count_neighboring_vps(self, links):
        df = pd.DataFrame(columns=['as1', 'as2', 'nb_vps'])
        fval = []
        findex = []

        for as1, as2 in links:
            nb_vps1 = self.number_of_ngh_vps(as1)
            nb_vps2 = self.number_of_ngh_vps(as2)
            df.loc[len(df)] = [as1, as2, min(len(nb_vps1), len(nb_vps2))]

        return df

if __name__ == "__main__":

    cnvp = CountNeighboringVPs('/root/type1_main/setup/db/full_topology/2022-01-12_full.txt')

    cnvp.construct_features()
    
        
    # vps_set = cnvp.number_of_ngh_vps(1491)
    # print (vps_set)