import subprocess
from io import StringIO
import pandas as pd
from colorama import Fore, Style
from itertools import groupby
import networkx as nx
import sys
from datetime import datetime
from time import mktime
import os



def err_msg(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.RED+Style.BRIGHT+"[ERROR ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)

def wrn_msg(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.YELLOW+Style.BRIGHT+"[WARNING ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)



def command_to_csv(args, debug=False):
    if debug:
        subprocess.run(args)
        return None

    res = subprocess.run(args, stdout=subprocess.PIPE)
    if res.returncode != 0:
        err_msg("Command \"{}\" failed".format(" ".join(args)))
        return None
    
    output = res.stdout.decode()

    s = ""
    for line in output.split("\n"):
        if "[" in line:
            continue
        s += line + "\n"


    io_data = StringIO(s)

    #try:
    df = pd.read_csv(io_data, sep=" ")
    # except:
    #     err_msg("Unable to transform command {} to CSV format".format(" ".join(args)))
    #     return None
    
    return df

def csv_to_string(df :pd.DataFrame):
    s = ""
    for i in range(0, len(df.index)):
        for feat in df.keys():
            if feat == "asp":
                s += "{}={}|".format(feat, df[feat].values[i].replace("|", "-"))
            else:
                s += "{}={}|".format(feat, df[feat].values[i])

        s = s.rstrip(s[-1])
        s += ","

    s = s.rstrip(s[-1])
    return s


####
# Function used to transform a string AS-path into a
# list of ASN (into string form). Also remove the prepending
# and process the AS aggragation if necessary.
#
# @param : path             String representing the AS-Path
#
####

def aspath_to_list(path):
    all_hops = []
    hops = [ k for k, g in groupby(path.replace('\n', '').split(" ")) ]
    #for all the elements in the array
    for i in range(0,len(hops)):
        # If the element is an AS aggregation, desagrege it
        all_hops.append(hops[i].replace('{', '').replace('}', '').split(","))

    return all_hops


def load_graph(fn, is_directed=False):
    if is_directed:
        G = nx.DiGraph()
    else:
        G = nx.Graph()

    with open(fn, "r") as f:
        for line in f:
            as1 = line.strip("\n").split(" ")[0]
            as2 = line.strip("\n").split(" ")[1]

            G.add_edge(as1, as2)

    return G


def create_directory(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


def get_all_dates(date_start, date_end, step=3600):
    all_dates = []
    start_ts = mktime(datetime.strptime(date_start, "%Y-%m-%dT%H:%M").timetuple())
    end_ts = mktime(datetime.strptime(date_end, "%Y-%m-%dT%H:%M").timetuple())

    cur_ts = start_ts

        # while all the hours are not visited,...
    while cur_ts < end_ts:
        start_tmp = datetime.utcfromtimestamp(cur_ts).strftime("%Y-%m-%dT%H:%M")
        stop_tmp = datetime.utcfromtimestamp(cur_ts + 3600).strftime("%Y-%m-%dT%H:%M")
        all_dates.append((start_tmp, stop_tmp))
            # ad a new process that will download all the events for the current houR
        cur_ts += step

    return all_dates


def parse_topo_file_line(line :str):
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


def load_all_ascones(date, db_dir):
    cones = dict()
    fn = "{}/cone/{}-{}-01.txt".format(db_dir, date.split("-")[0], date.split("-")[1])
    if not os.path.exists(fn):
        err_msg("Unable to find as rank file {} on local disk".format(fn))
        exit(1)

    with open(fn, "r") as f:
        for line in f:
            as1 = line.replace("\n", "").split(" ")[0]
            val = int(line.replace("\n", "").split(" ")[1])

            cones[as1] = val

    return cones


def test_all_keys_in_list(l1, l2, keys):
    for l in l2:
        if not test_all_keys_in_dic(l1, l, keys):
            return False

    return True

def test_all_keys_in_dic(l1, l2, keys):
    for k in keys:
        if l1[k] != l2[k]:
            return True

    return False

def remove_duplicated_dict(keys, l):
    res_list = []
    for i in range(len(l)):
        if test_all_keys_in_list(l[i], l[i+1:], keys):
            res_list.append(l[i])

    return res_list


def get_all_files_in_rep(rep):
    files = next(os.walk(rep), (None, None, []))[2]
    all_f = []

    # For all files in the directory,...
    for fil in files:
        all_f.append(rep + fil)

    return all_f


def prune_grip_aspath(asp, as1, as2):
    new_list = []
    path = aspath_to_list(asp)

    for i in range(0, len(path) - 1):
        for u in path[i]:
            new_list.append(u)
            for v in path[i+1]:
                if v == as1 and u == as2:
                    new_list.append(v)
                    return " ".join(new_list)

                elif v == as2 and u == as1:
                    new_list.append(v)
                    return " ".join(new_list)

    return None

if __name__ == "__main__":
    # test_list = [{"as1" : 1, "as2": 2, "value": 1}, {"as1" : 1, "as2": 2, "value": 5},
    #          {"as1" : 3, "as2": 2, "value": 1}, {"as1" : 5, "as2": 2, "value": 5}]
 
    # # printing original list
    # print ("Original list : " + str(test_list))
    
    # # using naive method to
    # # remove duplicates
    # res_list = []
    # for i in range(len(test_list)):
    #     if test_list[i] not in test_list[i + 1:]:
    #         res_list.append(test_list[i])

    # res_list = remove_duplicated_dict(["as1", "as2"], test_list)
    
    # # printing resultant list
    # print ("Resultant list : " + str(res_list))
    asp1 = "1 2 3 4 5 6"
    asp2 = "1 2 3 4 5 6 7"
    asp3 = "1 2 3 4 5 6 7 8"
    asp4 = "1 2 3 4 6 7 8"

    print(prune_grip_aspath(asp1, "5", "6"))
    print(prune_grip_aspath(asp2, "5", "6"))
    print(prune_grip_aspath(asp3, "5", "6"))
    print(prune_grip_aspath(asp4, "5", "6"))

    G = load_graph("/root/type1_main/setup/db/full_topology/2020-04-01_full.txt")
    print(G.degree["12389"])


