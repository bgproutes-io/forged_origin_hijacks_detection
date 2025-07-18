import os
import networkx as nx
import pandas as pd

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
    fn = "{}/full_topology/{}_full.txt".format(db_dir, date)
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

def merge_degree_cones(degrees :dict, cones :dict):
    res = []
    for node in degrees.keys():
        if node not in cones:
            cones[node] = 1

        res.append((degrees[node], cones[node], node))

    return res


def delete_tier_one_columns(df :pd.DataFrame):
    indexes = []
    for i in range(0, len(df.index)):
        if str(df["asn"].values[i]) in tier_one or int(df["cone"].values[i]) > 4400:
            indexes.append(i)

    df.drop(indexes, axis=0, inplace=True)
    
    return df