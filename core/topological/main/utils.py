import networkx as nx
from colorama import Fore, Style
from itertools import islice
import os
import sys
from datetime import datetime
from time import mktime


def err_msg(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.RED+Style.BRIGHT+"[ERROR ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


def wrn_msg(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.YELLOW+Style.BRIGHT+"[WARNING ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


def not_in_feat_to_remove(feat, feat_to_remove):

    for ftr in feat_to_remove:
        if feat in [ftr, ftr+"_as1", ftr+"_as2"]:
            return False

    return True


def parse_topo_file_line(line :str):
    as1 = line.strip("\n").split(" ")[0]
    as2 = line.strip("\n").split(" ")[1]

    if int(as1) > int(as2):
        as1, as2 = as2, as1

    return as1, as2

def load_topo_file(fn):
    G = nx.Graph()
    with open(fn, "r") as f:
        for line in f:
            as1, as2 = parse_topo_file_line(line)
            G.add_edge(as1, as2)

    return G


def divide_into_n_parts(lst, chunk):
    size = int(len(lst) / chunk) + 1
    lst = iter(lst)
    return list(iter(lambda: tuple(islice(lst, size)), ()))



def create_directory(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


def get_all_dates(date_start, date_end):
    all_dates = []
    start_ts = mktime(datetime.strptime(date_start, "%Y-%m-%d").timetuple())
    end_ts = mktime(datetime.strptime(date_end, "%Y-%m-%d").timetuple())

    cur_ts = start_ts

        # while all the hours are not visited,...
    while cur_ts <= end_ts:
        start_tmp = datetime.utcfromtimestamp(cur_ts).strftime("%Y-%m-%d")
        all_dates.append(start_tmp)
            # ad a new process that will download all the events for the current houR
        cur_ts += 60 * 60 * 24

    return all_dates

