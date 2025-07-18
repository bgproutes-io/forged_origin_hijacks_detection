import networkx as nx
import random 
import math
import utils as ut
from time import time
from colorama import Fore, Style
import sys
from datetime import datetime
import os



tier_one = ["174", "209", "286", "701", "1239", "1299", "2828", "2914", "3257", "3320", "3356", "3491", "5511", "6453", "6461", "6762", "6830", "7018", "12956", "6939", "1273", "9002", "4637", "7473"]


# Data from https://publicdata.caida.org/datasets/as-relationships/
def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN+Style.BRIGHT+"[Sampling.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)

##
# This function is used to load the existing aspaths in order to build
# the positive sampling (the hijacked links)
##

def load_existing_as_paths_positive_sampling(infile):
    if infile is None:
        ut.err_msg("Unable to load the aspath, no aspath file specified")
        exit(1)
    
    all_paths = dict()
    with open(infile, "r") as f:
        for line in f:
            asp = line.replace("\n", "")

            # Get the origin of the AS path
            path = ut.aspath_to_list(asp)
            if len(path) < 3:
                continue
            orig = path[-1][0]


            if orig not in all_paths:
                all_paths[orig] = []
            
            # Append this path to the list of path having the
            # same origin
            all_paths[orig].append(asp)

    return all_paths



def load_labels(date, db_dir):
    labels = dict()
    fn = "{}/sampling_cluster/{}.txt".format(db_dir, date)

    if not os.path.exists(fn):
        print_prefix("Need to compute the sampling clusters for {}".format(date))
        ut.get_clusters_for_date(date, db_dir)
        print_prefix("Sampling clusters for {} computed".format(date))

    max_lab = 0
    with open(fn, "r") as f:
        for line in f:
            if '#' not in line:
                asn = line.strip("\n").split(" ")[0]
                lab = int(line.strip("\n").split(" ")[1])

                labels[asn] = lab
                max_lab = max(max_lab, lab)

    return labels, max_lab+1


def positive_sampling_clusters(topo :nx.Graph, topo_irr, nb_link, date, db_dir, k=3./4., outfile=None, aspath_file=None):
    labels, max_lab = load_labels(date, db_dir)
    print_prefix("Labels loaded {} labels".format(max_lab))

    table_set = [set() for _ in range(0, max_lab)]
    table_proba = [0 for _ in range(0, max_lab * max_lab)]
    table_index = [i for i in range(0, max_lab * max_lab)]

    number_of_edges = 0

    if aspath_file is not None:
        start_ts = time()
        all_paths = load_existing_as_paths_positive_sampling(aspath_file)
        stop_ts = time()
        print_prefix("All aspaths loaded in {:.2f} s".format(stop_ts - start_ts))

    for n in topo.nodes():
        if n in labels:
            table_set[labels[n]].add(n)


    for e1, e2 in topo.edges():
        if e1 not in labels or e2 not in labels:
            continue
        if topo.degree[e1] <= topo.degree[e2]:
            as1 = e1
            as2 = e2
        else:
            as1 = e2
            as2 = e1

        index1 = labels[as1] * max_lab
        index2 = labels[as2]

        table_proba[index1+index2] += 1

        number_of_edges += 1

    edges_selected = set()
    all_edges = set()
    table_list = list(map(lambda x:list(random.sample(list(x), len(x))), table_set))
    table_proba = list(map(lambda x:math.pow(float(x)/float(number_of_edges), k), table_proba))
    sum_tmp = sum(table_proba)
    table_proba = list(map(lambda x:float(x)/float(sum_tmp), table_proba))


    for i in range(0, max_lab):
        for j in  range(i, max_lab):
            itern = 0
            size = 0
            while size < 100:
                good = False
                while good is False:
                    tmpnode1 = random.choices(table_list[i], k=1)[0]
                    tmpnode2 = random.choices(table_list[j], k=1)[0]

                    if tmpnode1 <= tmpnode2:
                        node1 = tmpnode1
                        node2 = tmpnode2
                    else:
                        node1 = tmpnode2
                        node2 = tmpnode1

                    if node1 != node2 and (node1, node2) not in all_edges and not topo.has_edge(node1, node2) and not topo_irr.has_edge(node1, node2):
                        if node1 in all_paths:
                            asp = random.choice(all_paths[node1])
                            edges_selected.add((node1, node2, asp))
                            all_edges.add((node1, node2))
                            good = True
                        elif node2 in all_paths:
                            asp = random.choice(all_paths[node2])
                            edges_selected.add((node2, node1, asp))
                            all_edges.add((node1, node2))
                            good = True
                        
                    # In case there is no new link found after 1000 iterations, we break and continue ..    
                    itern += 1
                    if itern >= 1000:
                        print_prefix ('No new link found after 1000 iterations')
                        break
                
                size += 1
                itern += 1
                if itern >= 1000:
                    print_prefix ('No new link found after 1000 iterations')
                    break
            
            print_prefix("Done for category {} {} !".format(i, j))


    s = ""
    for (as1, as2, asp) in edges_selected:
        s += "{} {},{} {}\n".format(as1, as2, asp, as2)

    if outfile is not None:
        with open(outfile, 'w', 1) as fd:
            fd.write(s)

    else:
        print(s, end="")
            

    return list(edges_selected)


##
# This function is used to load the existing aspaths in order to build
# the negative sampling (the legitimate links)
##

def load_existing_as_paths_negative_sampling(infile):
    if infile is None:
        ut.err_msg("Unable to load the aspath, no aspath file specified")
        exit(1)

    all_paths = dict()

    with open(infile, "r") as f:
        for line in f:

            asp = line.replace("\n", "")
            path = ut.aspath_to_list(asp)
            if len (path) > 3:

                # For all the links in the AS path:
                for i in range(0, len(path)-1):
                    for as1 in path[i]:
                        for as2 in path[i+1]:
                            tmp_as1 = as1
                            tmp_as2 = as2

                            if tmp_as1 > tmp_as2:
                                tmp_as2, tmp_as1 = tmp_as1, tmp_as2

                            if tmp_as2 not in all_paths:
                                all_paths[tmp_as2] = dict()

                            if tmp_as1 not in all_paths[tmp_as2]:
                                all_paths[tmp_as2][tmp_as1] = []

                            all_paths[tmp_as2][tmp_as1].append(asp)

    return all_paths


def negative_sampling_forced(topo :nx.Graph, nb_link, date, db_dir, k=1., outfile=None, aspath_file=None):
    labels, max_lab = load_labels(date, db_dir)
    print_prefix("Labels loaded {} labels".format(max_lab))

    table_set = [set() for _ in range(0, max_lab * max_lab)]
    table_proba = [0 for _ in range(0, max_lab * max_lab)]
    table_index = [i for i in range(0, max_lab * max_lab)]

    number_of_edges = 0

    if aspath_file is not None:
        start_ts = time()
        all_paths = load_existing_as_paths_negative_sampling(aspath_file)
        stop_ts = time()
        print_prefix("All aspaths loaded in {:.2f} s".format(stop_ts - start_ts))



    for e1, e2 in topo.edges():
        if e1 not in labels or e2 not in labels:
            continue

        if int(e1) > int(e2):
            as1 = e2
            as2 = e1
        else:
            as1 = e1
            as2 = e2

        index1 = labels[as1] * max_lab
        index2 = labels[as2]

        table_proba[index1+index2] += 1
        table_set[index1+index2].add((as1, as2))

        number_of_edges += 1

    edges_selected = set()
    sel_links = set()
    table_list = list(map(lambda x:list(random.sample(list(x), len(x))), table_set))
    table_proba = list(map(lambda x:math.pow(float(x)/float(number_of_edges), k), table_proba))
    sum_tmp = sum(table_proba)
    table_proba = list(map(lambda x:float(x)/float(sum_tmp), table_proba))


    for i in range(0, max_lab):
        for j in range(i, max_lab):
            index = i * max_lab + j
            if len(table_set[index]) < 100:
                print_prefix("For category {} {}, there are not 100 elements ({})".format(i, j, len(table_set[index])))
                for (as1, as2) in table_set[index]:

                    if as1 > as1:
                        as1, as2 = as2, as1

                    if as2 in all_paths and as1 in all_paths[as2]:
                        asp = random.choice(all_paths[as2][as1])
                        edges_selected.add((as1, as2, asp))
                        sel_links.add((as1, as2))
                        good = True

            else:
                print_prefix("For category {} {}, there are elements ({})".format(i, j, len(table_set[index])))
                size = 0
                sel_links = set()
                itern = 0
                
                while size < 100 and itern < 5000:
                    as1, as2 = random.choices(list(table_set[index]), k=1)[0]

                    if as1 > as1:
                        as1, as2 = as2, as1

                    if as1 != as2 and (as1, as2) not in sel_links:
                        if as2 in all_paths and as1 in all_paths[as2]:
                            asp = random.choice(all_paths[as2][as1])
                            edges_selected.add((as1, as2, asp))
                            sel_links.add((as1, as2))
                            size += 1
                        else:
                            itern += 1
                    else:
                        itern += 1

                if itern >= 5000:
                    print_prefix("Skipped because of itern")

            print_prefix("Done for category {} {} !".format(i, j))


    s = ""
    for (as1, as2, asp) in edges_selected:
        s += "{} {},{}\n".format(as1, as2, asp)

    if outfile is not None:
        with open(outfile, 'w', 1) as fd:
            fd.write(s)

    else:
        print(s, end="")
            

    return list(edges_selected)


if __name__ == "__main__":
    topo = ut.load_topo_file("/root/type1_main/setup/db/merged_topology/2023-02-01.txt")
    topo_irr = ut.load_topo_file("/root/type1_main/setup/db/irr/2023-02-01.txt")
    negative_sampling_forced(topo, 100, "2023-02-01", "/root/type1_main/setup/db", outfile="negative_eval_2023-02-01.txt", aspath_file="/root/type1_main/setup/db/paths/2023-02-01_paths.txt")
    positive_sampling_clusters(topo, topo_irr, 100, "2023-02-01", "/root/type1_main/setup/db", outfile="positive_eval_2023-02-01.txt", aspath_file="/root/type1_main/setup/db/paths/2023-02-01_paths.txt")

