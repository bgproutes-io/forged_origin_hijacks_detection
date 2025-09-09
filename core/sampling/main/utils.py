#import numpy as np
from itertools import groupby
import networkx as nx
from colorama import Fore, Style
import os
import sys
from datetime import datetime
from sklearn.cluster import KMeans
import pandas as pd
from datetime import datetime, timedelta


def aspath_to_list(path):
    all_hops = []
    hops = [ k for k, g in groupby(path.replace('\n', '').split(" ")) ]
    #for all the elements in the array
    for i in range(0,len(hops)):
        # If the element is an AS aggregation, desagrege it
        all_hops.append(hops[i].replace('{', '').replace('}', '').split(","))

    return all_hops


def list_to_aspath(l):
    path = [h[0] for h in l]
    return " ".join(path)

# This function fills a n*n table, where each cell is the number of links
# that belong to the corresponding category, according to the thresholds.
def compute_table(topo, thresholds, edges_file=None):

    # Initialisation of the table.
    table = [[0 for i in range(0, len(thresholds)-1)] for i in range(0, len(thresholds)-1)]

    if edges_file is None:
        edges_list = topo.edges()
    else:
        edges_list = []
        with open(edges_file, 'r') as fd:
            for line in fd.readlines():
                linetab = line.rstrip('\n').split(' ')
                as1 = int(linetab[0])
                as2 = int(linetab[1])
                edges_list.append((as1, as2))

    for as1, as2 in edges_list:
        if as1 in topo and as2 in topo:
            d1 = topo.degree(as1)
            d2 = topo.degree(as2)

            # Find in which category this link belong to.
            index1 = 0
            for i in range(0, len(thresholds)-1):
                if d1 > thresholds[i] and d1 < thresholds[i+1]:
                    index1 = i
                    break

            index2 = 0
            for i in range(0, len(thresholds)-1):
                if d2 > thresholds[i] and d2 < thresholds[i+1]:
                    index2 = i
                    break

            # Increment the corresponding value in the table.
            table[index1][index2] += 1
            if index1 != index2:
                table[index2][index1] += 1    

    # Transform row values into percentage.
    for t in table:
        for i in range(0, len(t)):
            if edges_file is None:
                t[i] = float("%.5f" %  (float(t[i])/float(topo.number_of_edges())))
            else:
                t[i] = float("%.5f" %  (float(t[i])/len(edges_list)))

        # t[i] = t[i].copy()


    return table



def err_msg(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.RED+Style.BRIGHT+"[ERROR ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


def parse_topo_file_line(line):
    as1 = line.strip("\n").split(" ")[0]
    as2 = line.strip("\n").split(" ")[1]

    return as1, as2

def load_topo_file(fn):
    G = nx.Graph()
    with open(fn, "r") as f:
        for line in f:
            as1, as2 = parse_topo_file_line(line)
            G.add_edge(as1, as2)

    return G

# Helper function to iterate between two dates.
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def load_suspicious_new_edge(db_dir, datestr, nbdays):
    date = datetime.strptime(datestr, "%Y-%m-%d")

    # Get the date for the first day of the X previous month.
    first_day = date - timedelta(days=nbdays)

    # All the suspicious cases detected the last nbdays days (to omit them).
    suspicious_edges = set()
    for cur_date in daterange(first_day, date):
        case_filename = db_dir+'/cases/'+cur_date.strftime("%Y-%m-%d")
        if os.path.isfile(case_filename):
            with open(case_filename, 'r') as fd:
                for line in fd.readlines():
                    if line.startswith('!sus'):
                        linetab = line.rstrip().split(' ')
                        as1 = linetab[1]
                        as2 = linetab[2]
                        suspicious_edges.add((as1, as2))

    return suspicious_edges

def create_directory(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


tier_one = ["174", "209", "286", "701", "1239", "1299", "2828", "2914", "3257", "3320", "3356", "3491", "5511", "6453", "6461", "6762", "6830", "7018", "12956", "6939", "1273", "9002", "4637", "7473"]

def get_all_cone_sizes(date, db_dir):
    fn = "{}/cone/{}-01.txt".format(db_dir, "-".join(date.split("-")[:2]))
    cones = dict()

    if os.path.exists(fn):
        with open(fn, "r") as f:
            for line in f:
                asn = line.strip("\n").split(" ")[0]
                cone_size = int(line.strip("\n").split(" ")[1])

                cones[asn] = cone_size

    else:
        print("Unable to find file {}".format(fn))

    return cones


def get_all_degrees(date, db_dir):
    fn = "{}/merged_topology/{}.txt".format(db_dir, date)
    G = nx.Graph()

    if os.path.exists(fn):
        with open(fn, "r") as f:
            for line in f:
                as1 = line.strip("\n").split(" ")[0]
                as2 = line.strip("\n").split(" ")[1]

                G.add_edge(as1, as2)

    else:
        print("Unable to find file {}".format(fn))

    degrees = dict()
    for node in G.nodes:
        degrees[node] = G.degree[node]

    return degrees

def merge_degree_cones(degrees :dict, cones :dict, topo, topo_irr):
    res = []
    tmp = dict()
    for node in degrees.keys():
        if node not in cones:
            cones[node] = 1

        tmp[node] = (degrees[node], cones[node])

    for node in cones.keys():
        if node not in degrees:
            degrees[node] = 1

        tmp[node] = (degrees[node], cones[node])

    for (node, (deg, cone)) in tmp.items():
        if topo.has_node(node) or topo_irr.has_node(node):
            res.append((deg, cone, node))

    return res


def delete_tier_one(df :pd.DataFrame):
    indexes = []
    for i in range(0, len(df.index)):
        if (str(df["asn"].values[i]) in tier_one or int(df["cone"].values[i]) > 4400):
            indexes.append(i)

    df.drop(indexes, axis=0, inplace=True)
    
    return df

def get_clusters_for_date(date, db_dir, topo, topo_irr, n_start=20):
    degrees = get_all_degrees(date, db_dir)
    cones = get_all_cone_sizes(date, db_dir)

    print ("len degrees {}".format(len(degrees)))
    print ("len cones {}".format(len(cones)))

    res = merge_degree_cones(degrees, cones, topo, topo_irr)

    print ("len res {}".format(len(cones)))

    deg = []
    cone = []
    asns = []

    for (d, c, asn) in res:
        deg.append(d)
        cone.append(c)
        asns.append(asn)

    cont = True
    while cont==True:
        df = pd.DataFrame()
        df["degree"] = deg
        df["cone"] = cone
        df["asn"] = asns
        df = delete_tier_one(df)

        model = KMeans(n_clusters=n_start, n_init="auto")
        clustering = model.fit(df.drop(columns=["asn"]))
        df["label"] = clustering.labels_

        cont = False
        for i in range(0, n_start):
            if len(df.loc[df['label']==i]) < 10:
                cont = True
                n_start -= 1
                break

    fn = "{}/sampling_cluster/{}.txt".format(db_dir, date)

    with open(fn, "w") as f:
        f.write('# Nb clusters: {} + Tier1 ASes\n'.format(n_start))

        for i in range(0, len(df.index)):
            f.write("{} {}\n".format(df["asn"].values[i], df["label"].values[i]))

        for to in tier_one:
            f.write("{} {}\n".format(to, n_start))


if __name__ == "__main__":
    get_clusters_for_date("2022-12-31", "/root/type1_main/setup/db/")