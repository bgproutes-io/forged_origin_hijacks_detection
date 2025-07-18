from cgitb import reset
from concurrent.futures import ProcessPoolExecutor
import compute_topo_features as cptf
import os
from colorama import Fore, Style
import utils as ut
import click
from time import time
import json
import sys
from datetime import datetime

import psycopg

def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN+Style.BRIGHT+"[topo_feat.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


def load_link_file(link_file):
    links = []
    if os.path.exists(link_file):
        with open(link_file, "r") as f:
            for line in f:
                if '#' in line:
                    continue
                if "," in line:
                    as1, as2 = ut.parse_topo_file_line(line.split(",")[0])
                else:
                    as1, as2 = ut.parse_topo_file_line(line)
                # Add only the uniq links
                if (as1, as2) not in links:
                    links.append((as1, as2))
    else:
        ut.err_msg("File {} has not been found in the local volume, skipped...".format(link_file))

    return links

def extract_links_from_db(date):
    links = []
    # Connect to the database
    conn = psycopg.connect(
        dbname=os.getenv("DFOH_DB_NAME"),
        user=os.getenv("DFOH_DB_USER"),
        password=os.getenv("DFOH_DB_PWD"),
        host="host.docker.internal",
        port=5432
    )
    with conn.cursor() as cur:
        # Get all the links for the date
        cur.execute("SELECT asn1, asn2 FROM new_link WHERE DATE(observed_at) = %s", (date,))
        rows = cur.fetchall()

        for row in rows:
            as1, as2 = row
            if as1 > as2:
                as1, as2 = as2, as1

            as1 = str(as1)
            as2 = str(as2)
            # Add only the uniq links
            if (as1, as2) not in links:
                links.append((as1, as2))
    conn.close()

    return links

# This array holds all the features that can be
# considered

all_feats = [
    "as1",
    "as2",
    "pagerank_as1",
    "pagerank_as2",
    "eigenvector_centrality_as1",
    "eigenvector_centrality_as2",
    "degree_centrality_as1",
    "degree_centrality_as2",
    "number_of_cliques_as1",
    "number_of_cliques_as2",
    "average_neighbor_degree_as1",
    "average_neighbor_degree_as2",
    "triangles_as1",
    "triangles_as2",
    "clustering_as1",
    "clustering_as2",
    "square_clustering_as1",
    "square_clustering_as2",
    "eccentricity_as1",
    "eccentricity_as2",
    "harmonic_centrality_as1",
    "harmonic_centrality_as2",
    "closeness_centrality_as1",
    "closeness_centrality_as2",
    "shortest_path",
    "jaccard",
    "adamic_adar",
    "preferential_attachement",
    "simrank_similarity"
    ]


# This class corresponds to the Topological features computer module
class TopoFeatComputer:
    def __init__(self, date, db_dir, feat_to_remove, max_workers, debug, overide, method):
        self.date = date                        # Date of the topology to load
        self.db_dir = db_dir                    # Database directory
        self.G = None                           # Topology
        self.feats = dict()                     # Feature values, as a result
        self.feat_to_remove = feat_to_remove    # Features to remove
        self.max_workers = max_workers          # Number of available threads
        self.debug = debug
        self.overide = overide
        self.method = method

        # Initialize the data structure for the feature results
        for feat in all_feats:
            # Skip the features to remove
            if ut.not_in_feat_to_remove(feat, feat_to_remove):
                self.feats[feat] = []

        try:
            if not os.path.isdir("{}/features/positive/topological_{}".format(self.db_dir, self.method)):
                ut.create_directory("{}/features/positive/topological_{}".format(self.db_dir, self.method))
            if not os.path.isdir("{}/features/negative/topological".format(self.db_dir)):
                ut.create_directory("{}/features/negative/topological".format(self.db_dir))
        except FileNotFoundError:
            ut.err_msg("Database directories could not be created, are you sure your database is located at \"{}\" ?".format(self.db_dir))
            exit(1)

    
    def load_data(self):
        fn_topo = "{}/merged_topology/{}.txt".format(self.db_dir, self.date)

        # If the file representing the topology has been found,
        if os.path.exists(fn_topo):
            print_prefix("Loading topology graph...")

            start_ts = time()
            # Then load it
            self.G = ut.load_topo_file(fn_topo)
            stop_ts = time()

            print_prefix("Topology loaded in {:.4f} s ({} links)".format(stop_ts - start_ts, len(self.G.edges)))

        # Else, raise an error
        else:
            ut.err_msg("Unable to locally find file {}".format(fn_topo))
            exit(1)
            


    ####
    # Function used to compute the topological-based features for one single link
    ####

    def compute_one_link(self, as1, as2):
        ret = False

        # First check if both nodes are in the graph

        # If not, exit
        if ret:
            return None

        cptf.warning_ = self.debug

        topo = self.G.copy()

        # Remove the edge (only if it is a negative sample)
        if topo.has_edge(as1, as2):
            topo.remove_edge(as1, as2)

        # Compute the features without the edge
        if as1 not in self.G.nodes or as2 not in self.G.nodes:
            ut.wrn_msg("Either {} or {} is not in G".format(as1, as2))
            feats_before = cptf.res_zero
        else:
            feats_before = cptf.compute_all_features(topo, as1, as2, feat_exclude=self.feat_to_remove)

        if None in list(feats_before.values()):
            feats_before = cptf.res_zero

        # add the edge
        topo.add_edge(as1, as2)

        # Compute the features after the edge apears
        feats_after = cptf.compute_all_features(topo, as1, as2, feat_exclude=self.feat_to_remove)

        if None in list(feats_after.values()):
            return None

        diff_feat = dict()

        # Compute the difference between each feature
        for feat in feats_after.keys():
            if feat not in ["as1", "as2"]:
                diff_feat[feat] = feats_after[feat] - feats_before[feat]

        # append the ases
        diff_feat["as1"] = as1
        diff_feat["as2"] = as2


        return diff_feat

    
    def compute_bunch_links(self, links):
        all_res = []

        for (as1, as2) in links:
            all_res.append((self.compute_one_link(as1, as2), as1, as2))

        return all_res

    ####
    # Function used to compute the topological features for 
    # all the links in the list
    ####

    def compute_multiple_links(self, link_list):

        # If there is only one worker or only one elem in the
        # list of links, process it sequentially
        start = time()
        if self.max_workers == 1 or len(link_list) <= 1:
            res = self.compute_bunch_links(link_list)

            # For each edge in result
            for (feats, as1, as2) in res:
                # For each feature of the edge
                if feats is None:
                    ut.err_msg("Link {} {} cannot be computed because either {} or {} are not in G, skipped...".format(as1, as2, as1, as2))
                else:
                    if None in list(feats.values()):
                        ut.err_msg("Link {} {} cannot be computed because of a Networkx error, skipped...".format(as1, as2))
                    else:
                        for (feat, val) in feats.items():
                            self.feats[feat].append(val)

        # Else, process it on multiple threads
        else:
            all_chunks = ut.divide_into_n_parts(link_list, self.max_workers)

            proc_list = []

            with ProcessPoolExecutor(max_workers=self.max_workers) as exec:
                for i in range(0, len(all_chunks)):
                    proc_list.append(exec.submit(self.compute_bunch_links, list(all_chunks[i])))

                for p in proc_list:
                    res = p.result()

                    # For each edge in result
                    for (feats, as1, as2) in res:
                        # For each feature of the edge
                        if feats is None:
                            ut.err_msg("Link {} {} cannot be computed because either {} or {} are not in G, skipped...".format(as1, as2, as1, as2))
                        else:
                            if None in list(feats.values()):
                                ut.err_msg("Link {} {} cannot be computed because of a Networkx error, skipped...".format(as1, as2))
                            else:
                                for (feat, val) in feats.items():
                                    self.feats[feat].append(val)

        tick = time() - start
        print_prefix("Link list for day {} took {:.4f} s".format(self.date, tick))


    def clear(self):
        for feat in all_feats:
            # Skip the features to remove
            if ut.not_in_feat_to_remove(feat, self.feat_to_remove):
                self.feats[feat] = []
    

    def build_daily_sampling(self):
        pos_sampling = "{}/sampling/positive/sampling_{}/{}_positive.txt".format(self.db_dir, self.method, self.date)
        neg_sampling = "{}/sampling/negative/sampling/{}_negative.txt".format(self.db_dir, self.date)

        fn_pos = "{}/features/positive/topological_{}/{}_positive.txt".format(self.db_dir, self.method, self.date)

        # Process this part only if it does not exists
        if os.path.exists(fn_pos) and not self.overide:
            print_prefix("positive topological features for day {} already exists, skipped...".format(self.date))
        else:
            links = load_link_file(pos_sampling)
            self.compute_multiple_links(links)

            with open(fn_pos, "w") as f:
                f.write(self.to_string(None))

        self.clear()

        
        fn_neg = "{}/features/negative/topological/{}_negative.txt".format(self.db_dir, self.date)

        # Process this part only if it does not existss
        if os.path.exists(fn_neg) and not self.overide:
            print_prefix("negative topological features for day {} already exists, skipped...".format(self.date))
        else:
            links = load_link_file(neg_sampling)
            self.compute_multiple_links(links)

            with open(fn_neg, "w") as f:
                f.write(self.to_string(None))




    ####
    # Transform the results into string
    ####
    def to_string(self, label):
        s = ""

        line = []
        all_feats = list(self.feats.keys())
        for feat in all_feats:
            line.append(feat)
        
        if label is None:
            s += "{}\n".format(" ".join(line))
        else:
            s += "{} label\n".format(" ".join(line))

        for i in range(0, len(self.feats["as1"])):
            line = []
            for feat in all_feats:
                line.append(str(self.feats[feat][i]))
            
            if label is None:
                s += "{}\n".format(" ".join(line))
            else:
                s += "{} {}\n".format(" ".join(line), label)


        return s


    def to_json(self, label):
        res = []
        all_feats = list(self.feats.keys())
        
        for i in range(0, len(self.feats["as1"])):
            elem = dict()
            for feat in all_feats:
                elem[feat] = self.feats[feat][i]
            
            if label is not None:
                elem["label"] = label
            res.append(elem.copy())

        

        return json.dumps(res)



def topo_feat_aux(date, db_dir, nb_threads, feat_exclude, debug, overide, method):
    tfc = TopoFeatComputer(date, db_dir, feat_exclude.split(","), nb_threads, debug, overide, method)
    tfc.load_data()

    tfc.build_daily_sampling()




@click.command()
@click.option("--date", help="Date of the link appearance, used to load the right topology", type=str)
@click.option("--outfile", default=None, help="file where to write the results of the feature computation", type=str)
@click.option("--nb_threads", default=2, help="Number of threads to use to compute the graph features", type=int)
@click.option("--db_dir", default="/tmp/db", help="Database Directory", type=str)
@click.option("--feat_exclude", default="pagerank,eigenvector_centrality,square_clustering,number_of_cliques,simrank_similarity", help="Features to exclude during the computation", type=str)
@click.option('--store_results_in_db', is_flag=True, help='If set, store the results in the PostgreSQL database.')
#example with 57695-41484,7545-63956,41805-8683
@click.option("--link_list", default=None, help="List of links to test, in the form \"as1-as2,as3-as4,as5-as6\"", type=str)
@click.option("--link_file", default=None, help="file with the links to read. Each line of the file must be on the form \"as1 as2,whatever you want\" or \"as1 as2 whatever you want\". Basically, these files corresponds to the sampling files", type=str)
@click.option("--label", default=None, help="Label to assign to each link", type=int)
@click.option("--json_dump", default=0, help="Choose to dump in a JSON format", type=int)
@click.option("--daily_sampling", default=0, help="Builds the daily sampling, in terms of positive and negative samples. Should be passed with option --date", type=int)
@click.option("--debug", default=0, help="Enable feature computation debugging", type=int)
@click.option("--overide", default=0, help="overide topological feature files", type=int)
@click.option("--end_date", default=None, help="End date of bunch", type=str)
@click.option("--method", default="clusters", help="Sampling method used", type=str)


def run_orchestrator(date, \
                     outfile, \
                     nb_threads, \
                     db_dir, \
                     feat_exclude, \
                     store_results_in_db, \
                     link_list, \
                     link_file, \
                     label, \
                     json_dump, \
                     daily_sampling, \
                     debug, \
                     overide, \
                     end_date, \
                     method):
    if date is None:
        ut.err_msg("Please enter a date with option --date")
        exit(1)


    if end_date is not None:
        all_dates = ut.get_all_dates(date, end_date)
        print(all_dates)
        all_procs = []

        with ProcessPoolExecutor(max_workers=nb_threads) as exec:
            for d in all_dates:
                all_procs.append(exec.submit(topo_feat_aux, d, db_dir, 1, feat_exclude, debug, overide, method))

            for p in all_procs:
                p.result()

    # If no input are provided, raise an error
    if link_file is None and link_list is None and not daily_sampling and not store_results_in_db:
        ut.err_msg("You must provide either a link file, a link list, set --store_results_in_db or enable daily sampling with --daily_sampling=1")
        exit(1)

    tfc = TopoFeatComputer(date, db_dir, feat_exclude.split(","), nb_threads, debug, overide, method)
    tfc.load_data()

    if daily_sampling:
        tfc.build_daily_sampling()
        exit(0)

    links = []
    if store_results_in_db:
        links = extract_links_from_db(date)

    # Add all the links of the option --link_file
    elif link_file:
        links = load_link_file(link_file)

    # Add all the links of the option --link_list
    elif link_list:
        all_l = link_list.split(",")
        for l in all_l:
            as1 = l.split("-")[0]
            as2 = l.split("-")[1]

            if int(as1) > int(as2):
                as1, as2 = as2, as1

            link = (as1, as2)
            # Add only the uniq links
            if link not in links:
                links.append(link)
 

    # If the link list is still empty, raise an error
    if len(links) == 0:
        ut.err_msg("No links in any provided input")
        exit(1)


    tfc.compute_multiple_links(links)


    if outfile is not None:
        with open(outfile, "w") as f:
            if json_dump:
                f.write(tfc.to_json(label))
            else:
                f.write(tfc.to_string(label))
    else:
        if json_dump:
            print(tfc.to_json(label))
        else:
            print(tfc.to_string(label))


if __name__ == "__main__":
    run_orchestrator()
