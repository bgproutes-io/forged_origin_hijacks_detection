import networkx as nx
import random 
import math
import utils as ut
from time import time
from colorama import Fore, Style
import sys
from datetime import datetime
import os


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



##
# This function is used to load the existing aspaths in order to build
# the negative sampling (the legitimate links)
##

def load_possible_as_paths_negative_sampling(infile):
    if infile is None:
        ut.err_msg("Unable to load the aspath, no aspath file specified")
        exit(1)

    all_paths = dict()

    with open(infile, "r") as f:
        for line in f:

            asp = line.replace("\n", "")
            path = ut.aspath_to_list(asp)
            if len (path) > 2:

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
                                all_paths[tmp_as2][tmp_as1] = True

    return all_paths


##
# This Function is used to build a negative sampling, that is the
# set of existing link that must NOT raise any alarm.
##

def negative_sampling(topo, nb_link, outfile=None, aspath_file=None):
    
    selected_links = set()
    sel = set()
    start_ts = time()
    all_paths = load_existing_as_paths_negative_sampling(aspath_file)
    stop_ts = time()
    print_prefix("All aspaths loaded in {:.2f} s".format(stop_ts - start_ts))
    

    while len(selected_links) < nb_link:
        as1, as2 = random.sample(topo.edges(), 1)[0]
        if as1 > as2:
            # Switch the two variables to follow same convention in the code.
            as1, as2 = as2, as1

        # Check if the removing of the link between as1 and as2 keeps the graph connexe.
        if as2 not in all_paths:
            print("{} is not in any AS-path".format(as2))
        elif as1 not in all_paths[as2]:
            print("Link {} {} is not in any path...".format(as1, as2))
        
        if (as1, as2) not in sel and as2 in all_paths and as1 in all_paths[as2]:
            topo.remove_edge(as1,as2)
            res = nx.has_path(topo, as1, as2)
            topo.add_edge(as1,as2)
            if res:
                asp = random.choice(all_paths[as2][as1])
                selected_links.add((as1, as2, asp))
                sel.add((as1, as2))

                if len(selected_links)%20 == 0:
                    print("There are {} selected links".format(len(selected_links)))
        # elif as2 in all_paths and as1 in all_paths[as2]:
        #     topo.remove_edge(as1,as2)
        #     res = nx.has_path(topo, as1, as2)
        #     topo.add_edge(as1,as2)
        #     if res:
        #         asp = random.choice(all_paths[as2][as1])
        #         selected_links.add((as1, as2, asp))


    s = ""
    for (as1, as2, asp) in selected_links:
        s += "{} {},{}\n".format(as1, as2, asp)
                
    if outfile != None:
        with open(outfile, "w") as f:
            f.write(s)
    
    else:
        print(s, end="")



    return list(selected_links)


def positive_sampling_random(topo :nx.Graph, topo_irr :nx.Graph, nb_link, outfile, aspath_file):
    start_ts = time()
    all_paths = load_existing_as_paths_positive_sampling(aspath_file)
    stop_ts = time()
    print_prefix("All aspaths loaded in {:.2f} s".format(stop_ts - start_ts))

    edges_selected = set()
    sel = set()

    while len(edges_selected) < nb_link:
        as1 = random.choice(list(topo.nodes))
        as2 = random.choice(list(topo.nodes))

        if as1 == as2 or topo.has_edge(as1, as2) or topo_irr.has_edge(as1, as2) or (as1, as2) in sel:
            continue

        if as1 in all_paths:
            asp = random.choice(all_paths[as1])
            edges_selected.add((as1, as2, asp))
            sel.add((as1, as2))

        elif as2 in all_paths:
            asp = random.choice(all_paths[as2])
            edges_selected.add((as2, as1, asp))
            sel.add((as1, as2))

    s = ""
    for (as1, as2, asp) in edges_selected:
        if int(as1) > int(as2):
            tmp_as1 = as2
            tmp_as2 = as1

        else: 
            tmp_as1 = as1
            tmp_as2 = as2

        s += "{} {},{} {}\n".format(tmp_as1, tmp_as2, asp, as2)

    if outfile is not None:
        with open(outfile, 'w', 1) as fd:
            fd.write(s)

    else:
        print(s, end="")
            

    return list(edges_selected)




# This functions returns a negative sample (ie that contains non existing links).
# Yet, it does not do that randomly but it returns a sample that is balanced, ie
# that contains equally distributed in different categories. 
def positive_sampling_thresholds(topo, topo_irr, nb_link, k=3./4., outfile=None, aspath_file=None, thresholds=None):
    if thresholds is None:
        thresholds = [0, 10, 50, 100, 500, 1000, 1500, 3000, 5000, 100000]
    
    table_set = [set() for i in range(0, (len(thresholds)-1))]
    table_proba = [0 for i in range(0, (len(thresholds)-1)*(len(thresholds)-1))]
    table_index = [i for i in range(0, (len(thresholds)-1)*(len(thresholds)-1))]

    number_of_edges = 0

    start_ts = time()
    all_paths = load_existing_as_paths_positive_sampling(aspath_file)
    stop_ts = time()
    print_prefix("All aspaths loaded in {:.2f} s".format(stop_ts - start_ts))

    # Placing every node in its corrsponding degree category.
    for n in topo.nodes():
        for i in range(0, len(thresholds)-1):
            if topo.degree[n] >= thresholds[i] and topo.degree[n] < thresholds[i+1]:
                table_set[i].add(n)
                break

    # Calculating the edge distribution.
    for e1, e2 in topo.edges():
        if topo.degree[e1] <= topo.degree[e2]:
            as1 = e1
            as2 = e2
        else:
            as1 = e2
            as2 = e1

        d1 = topo.degree[as1]
        d2 = topo.degree[as2]

        index1 = 0
        for i in range(0, len(thresholds)-1):
            if d1 >= thresholds[i] and d1 < thresholds[i+1]:
                index1 = i*(len(thresholds)-1)
                break

        index2 = 0
        for i in range(0, len(thresholds)-1):
            if d2 >= thresholds[i] and d2 < thresholds[i+1]:
                index2 = i
                break
            
        table_proba[index1+index2] += 1

        number_of_edges += 1
        # if number_of_edges == 10000:
        #     break


    # Generating the sampling.
    edges_selected = set()
    table_list = list(map(lambda x:list(random.sample(list(x), len(x))), table_set))
    table_proba = list(map(lambda x:math.pow(float(x)/float(number_of_edges), k), table_proba))
    sum_tmp = sum(table_proba)
    table_proba = list(map(lambda x:float(x)/float(sum_tmp), table_proba))
    sel = set()
    # Print the probability table.
    s = ''
    for i in range(0, len(thresholds)-1):
        for j in range(0, len(thresholds)-1):
            s = s+'{} '.format(table_proba[i*(len(thresholds)-1)+j])
        s += '\n'

    # Generate the final sample.
    while len(edges_selected) < nb_link:
        good = False
        index_selected = random.choices(table_index, k=1, weights=table_proba)[0]

        threshold_set_node1 = int((index_selected - index_selected%(len(thresholds)-1))/(len(thresholds)-1))
        threshold_set_node2 = int(index_selected%(len(thresholds)-1))

        itern = 0
        while good is False:

            tmpnode1 = random.choices(table_list[threshold_set_node1], k=1)[0]
            tmpnode2 = random.choices(table_list[threshold_set_node2], k=1)[0]

            if tmpnode1 <= tmpnode2:
                node1 = tmpnode1
                node2 = tmpnode2
            else:
                node1 = tmpnode2
                node2 = tmpnode1

            if node1 != node2 and (node1, node2) not in sel and not topo.has_edge(node1, node2) and not topo_irr.has_edge(node1, node2):
                if node1 in all_paths:
                    asp = random.choice(all_paths[node1])
                    edges_selected.add((node1, node2, asp))
                    sel.add((node1, node2))
                    good = True
                elif node2 in all_paths:
                    asp = random.choice(all_paths[node2])
                    edges_selected.add((node2, node1, asp))
                    sel.add((node1, node2))
                    good = True

            # In case there is no new link found after 1000 iterations, we break and continue ..    
            itern += 1
            if itern == 1000:
                print_prefix ('No new link found after 1000 iterations')
                break
        # if len(edges_selected) % 1000 == 0:
        #     print ("Creation of the positive sample (normal): {}". format(len(edges_selected)))

    s = ""
    for (as1, as2, asp) in edges_selected:
        s += "{} {},{} {}\n".format(as1, as2, asp, as2)

    if outfile is not None:
        with open(outfile, 'w', 1) as fd:
            fd.write(s)

    else:
        print(s, end="")
            

    return list(edges_selected)


def load_labels(date, db_dir, topo, topo_irr, overide=False):
    labels = dict()
    fn = "{}/sampling_cluster/{}.txt".format(db_dir, date)

    if not os.path.exists(fn) or overide:
        print_prefix("Need to compute the sampling clusters for {}".format(date))
        ut.get_clusters_for_date(date, db_dir, topo, topo_irr)
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


def positive_sampling_clusters(topo :nx.Graph, topo_irr, nb_link, date, db_dir, k=3./4., outfile=None, aspath_file=None, overide=0):
    labels, max_lab = load_labels(date, db_dir, topo, topo_irr, overide=overide)

    # We generate three distinct max_lab * max_lab grids.
    # The first stores in every cell the set of AS nodes.
    table_set = [set() for _ in range(0, max_lab)]
    # The second one stores the probability to see every AS link category.
    table_proba = [0 for _ in range(0, max_lab * max_lab)]
    # The third one stores the index in every cell (the random choices function is run on that table).
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
    table_list = list(map(lambda x:list(random.sample(list(x), len(x))), table_set))
    table_proba = list(map(lambda x:math.pow(float(x)/float(number_of_edges), k), table_proba))
    sum_tmp = sum(table_proba)
    table_proba = list(map(lambda x:float(x)/float(sum_tmp), table_proba))



    while len(edges_selected) < nb_link:
        good = False
        index_selected = random.choices(table_index, k=1, weights=table_proba)[0]

        threshold_set_node1 = int((index_selected - index_selected%max_lab)/max_lab)
        threshold_set_node2 = int(index_selected%max_lab)

        itern = 0
        while good is False:

            tmpnode1 = random.choices(table_list[threshold_set_node1], k=1)[0]
            tmpnode2 = random.choices(table_list[threshold_set_node2], k=1)[0]

            if tmpnode1 <= tmpnode2:
                node1 = tmpnode1
                node2 = tmpnode2
            else:
                node1 = tmpnode2
                node2 = tmpnode1

            if node1 != node2 and (node1, node2) not in edges_selected and not topo.has_edge(node1, node2) and not topo_irr.has_edge(node1, node2):  
                if aspath_file is not None:
                    if node1 in all_paths:
                        asp = random.choice(all_paths[node1])
                        edges_selected.add((node1, node2, asp))
                        good = True
                    elif node2 in all_paths:
                        asp = random.choice(all_paths[node2])
                        edges_selected.add((node2, node1, asp))
                        good = True
                else:
                    asp = ""
                    edges_selected.add((node1, node2, asp))
                    good = True
                

            # In case there is no new link found after 1000 iterations, we break and continue ..    
            itern += 1
            if itern == 1000:
                print_prefix ('No new link found after 1000 iterations')
                break
        # if len(edges_selected) % 1000 == 0:
        #     print ("Creation of the positive sample (normal): {}". format(len(edges_selected)))

    s = ""
    for (as1, as2, asp) in edges_selected:
        s += "{} {},{} {}\n".format(as1, as2, asp, as2)

    if outfile is not None:
        with open(outfile, 'w', 1) as fd:
            fd.write(s)

    else:
        print(s, end="")
            

    return list(edges_selected)




def negative_sampling_forced(topo :nx.Graph, topo_irr: nx.Graph, nb_link, date, db_dir, k=1., outfile=None, aspath_file=None):    
    labels, max_lab = load_labels(date, db_dir, topo, topo_irr)

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

    while len(edges_selected) < nb_link:
        good = False
        index_selected = random.choices(table_index, k=1, weights=table_proba)[0]

        # threshold_set_node1 = int((index_selected - index_selected%max_lab)/max_lab)
        # threshold_set_node2 = int(index_selected%max_lab)

        itern = 0
        while good is False:
            # If there is no existing link in this inter-cluster category, we move to the next iteration.
            if len(table_list[index_selected]) == 0:
                break

            tmpnode1, tmpnode2 = random.choices(table_list[index_selected], k=1)[0]

            topo.remove_edge(tmpnode1, tmpnode2)
            res = nx.has_path(topo, tmpnode1, tmpnode2)
            topo.add_edge(tmpnode1, tmpnode2)

            if not res:
                continue

            if tmpnode1 <= tmpnode2:
                node1 = tmpnode1
                node2 = tmpnode2
            else:
                node1 = tmpnode2
                node2 = tmpnode1

            if node1 != node2 and (node1, node2) not in sel_links:  
                if aspath_file is not None:
                    if node2 in all_paths and node1 in all_paths[node2]:
                        asp = random.choice(all_paths[node2][node1])
                        edges_selected.add((node1, node2, asp))
                        sel_links.add((node1, node2))
                        good = True
                else:
                    asp = ""
                    edges_selected.add((node1, node2, asp))
                    sel_links.add((node1, node2))
                    good = True
                

            # In case there is no new link found after 1000 iterations, we break and continue ..    
            itern += 1
            if itern == 100:
                print_prefix ('No new link found after 100 iterations')
                break
        # if len(edges_selected) % 1000 == 0:
        #     print ("Creation of the positive sample (normal): {}". format(len(edges_selected)))

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
    date = "2022-04-30"
    db_dir = "/root/type1_main/setup/db"
    topo_file = "{}/full_topology/{}_full.txt".format(db_dir, date)
    asp_file = "{}/paths/{}-01_paths.txt".format(db_dir, "-".join(date.split("-")[:2]))
    G = ut.load_topo_file(topo_file)

    negative_sampling_forced(G, 2000, date, db_dir, outfile="test.txt", aspath_file=asp_file)
