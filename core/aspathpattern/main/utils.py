import os
from colorama import Fore, Style
from itertools import groupby
import pandas as pd
import networkx as nx
import os
import datetime
import sys
from time import mktime

import psycopg

cones = dict()
degrees = dict()

####
# Function used to load the mapping between the AS degree and its ASN
####

def load_all_degrees(date, db_dir):
    fn = "{}/merged_topology/{}.txt".format(db_dir, date)
    if not os.path.exists(fn):
        err_msg("Unable to find degree file {} on local disk".format(fn))
        exit(1)

    G = nx.Graph()

    with open(fn, "r") as f:
        for line in f:
            as1 = line.replace("\n", "").split(" ")[0]
            as2 = line.replace("\n", "").split(" ")[1]

            G.add_edge(as1, as2)

    for node in G.nodes:
        degrees[node] = G.degree[node]


####
# Function used to load the mapping between the AS degree and its Customer
# cone size
####

def load_all_ascones(date, db_dir):
    fn = "{}/cone/{}-{}-01.txt".format(db_dir, date.split("-")[0], date.split("-")[1])
    if not os.path.exists(fn):
        err_msg("Unable to find as rank file {} on local disk".format(fn))
        exit(1)

    with open(fn, "r") as f:
        for line in f:
            as1 = line.replace("\n", "").split(" ")[0]
            val = int(line.replace("\n", "").split(" ")[1])

            cones[as1] = val

    


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



####
# Transform a list of ASN (string form) into a list of
# relation between the adjacent AS regarding the metric.
# For instance, if metric is cone, the list [as1, as2, as3]
# will be transformed into:
#
# [Cone_size(as1) - Cone_size(as2), Cone_size(as2) - Cone_size(as3)]
#
# @param : path             List of Stringed ASN
# @param : metric           Considered metric (either cone or degree)
#
####

def aspath_to_rel_list(path, metric="cone"):
    
    links = []
    for i in range(1, len(path)):
        u = path[i-1][0]
        v = path[i][0]

        # When the metric is cone, use the customer cone size
        if metric == "cone":
            try:
                m_u = cones[u]
            except:
                m_u = 1

            try:
                m_v = cones[v]
            except:
                m_v = 1

        # When the metric is degree use the AS degree
        elif metric == "degree":
            try:
                m_u = degrees[u]
            except:
                m_u = 1

            try:
                m_v = degrees[v]
            except:
                m_v = 1

        links.append(m_u - m_v)

    return links



####
# Function used only when running the aspath pattern features alone.
# Compute the accuracy, the TPR and the FPR of the model.
#
# @param : pred             result of model.predict
# @param : Y                real label of testing set
#
####

def compute_prediction_stats(pred, Y):
    FN = 0
    TN = 0
    TP = 0
    FP = 0

    if len(pred) != len(Y):
        print("Error inconsistent length of vectors", file=sys.stderr)
        exit(1)

    for i in range(0, len(Y)):
        if Y[i] == 1:
            if pred[i] == 1:
                TP += 1
            else:
                FN += 1

        else:
            if pred[i] == 1:
                FP += 1
            else:
                TN += 1

    print("Stats:\nAccuracy : {:.2f}\nTPR : {:.2f}\nFPR : {:.2f}\nTP : {}\nFP : {}\nFN : {}\nTN : {}".format(
        (TP + TN) / (TP + TN + FP + FN) * 100, TP / (TP + FN) * 100, FP / (TN + FP) * 100, TP, FP, FN, TN
    ))  


####
# Function used to write the results for the aspath pattern
# inference, in terms of probability to be a positive sample.
#
# @param : X            Testing features (used only to get as1 and as2)
# @param : pred_degree  Prediction of aspath patterns when considering AS degree
# @param : pred_cone    Prediction of aspath patterns when considering Customer Cone Size
# @param : outfile      Output file (String)
#
####

def write_metric_values(X:pd.DataFrame, pred_degree, pred_cone, outfile):
    with open(outfile, "w") as f:
        for i in range(0, len(X.index)):
            f.write("{} {} {} {}\n".format(X["as1"].values[i], X["as2"].values[i], pred_degree[i][1], pred_cone[i][1]))


####
# Function only used for debug, allows to print the predictions
# in the corresponding file
####

def print_false_predictions(pred, Y, X):

    fpos = open("tmp/false_positives.txt", "w")
    fneg = open("tmp/false_negatives.txt", "w")
    tpos = open("tmp/true_positives.txt", "w")
    tneg = open("tmp/true_negatives.txt", "w")

    for i in range(0, len(Y)):
        as1 = X["as1"].values[i]
        as2 = X["as2"].values[i]
        if Y[i] == 1:
            if pred[i] == 0:
                fneg.write("{} {}\n".format(as1, as2))
            
            else:
                tpos.write("{} {}\n".format(as1, as2))

        else:
            if pred[i] == 1:
                fpos.write("{} {}\n".format(as1, as2))

            else:
                tneg.write("{} {}\n".format(as1, as2))

            

    fpos.close()
    fneg.close()
    tpos.close()
    tneg.close()




def err_msg(msg, end="\n"):
    currentTime = datetime.datetime.now().strftime("%H:%M:%S")
    s = Fore.RED+Style.BRIGHT+"[ERROR ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)

def wrn_msg(msg, end="\n"):
    currentTime = datetime.datetime.now().strftime("%H:%M:%S")
    s = Fore.YELLOW+Style.BRIGHT+"[WARNING ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


####
# Function that transform a file with aspaths into
# a list of aspaths.
####

def file_to_aspaths_list(fn):
    aspaths = []
    if os.path.exists(fn):
        nbLine  = 0
        with open(fn, "r") as f:
            for line in f:
                
                if '#' in line:
                    continue

                nbLine += 1
                try:
                    as1 = line.strip("\n").split(",")[0].split(" ")[0]
                    as2 = line.strip("\n").split(",")[0].split(" ")[1]
                    asp = line.strip("\n").split(",")[1]

                    if int(as1) > int(as2):
                        as1, as2 = as2, as1

                    aspaths.append((as1, as2, asp))

                except:
                    wrn_msg("File {} has an incorrect format at line {}, skipped...".format(fn, nbLine))


    else:
        err_msg("File {} does not exists".format(fn))

    return aspaths

def extract_aspath_list_from_db(date):
    aspaths = []
    conn = psycopg.connect(
        dbname=os.getenv("DFOH_DB_NAME"),
        user=os.getenv("DFOH_DB_USER"),
        password=os.getenv("DFOH_DB_PWD"),
        host="host.docker.internal",
        port=5432
    )
    with conn.cursor() as cur:
        cur.execute("SELECT asn1, asn2, as_path FROM new_link WHERE DATE(observed_at) = %s", (date,))
        rows = cur.fetchall()
        for row in rows:
            as1 = row[0]
            as2 = row[1]
            asp = row[2]

            if as1 > as2:
                as1, as2 = as2, as1

            aspaths.append((str(as1), str(as2), asp))
    conn.close()
    return aspaths


def get_the_last_n_days(d, n):
    dates = []
    tod = datetime.date.fromisoformat(d)
    for i in range(1, n+1):
        delta = datetime.timedelta(days = i)
        dates.append(str(tod - delta))

    return dates


def create_directory(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)



def get_all_dates(date_start, date_end):
    all_dates = []
    start_ts = mktime(datetime.datetime.strptime(date_start, "%Y-%m-%d").timetuple())
    end_ts = mktime(datetime.datetime.strptime(date_end, "%Y-%m-%d").timetuple())

    cur_ts = start_ts

        # while all the hours are not visited,...
    while cur_ts <= end_ts:
        start_tmp = datetime.datetime.utcfromtimestamp(cur_ts).strftime("%Y-%m-%d")
        all_dates.append(start_tmp)
            # ad a new process that will download all the events for the current houR
        cur_ts += 60 * 60 * 24

    return all_dates

