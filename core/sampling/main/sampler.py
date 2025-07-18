import networkx as nx
import utils as ut
from time import time
import os
from colorama import Fore, Style
import sampling
import click
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
from time import mktime


def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN+Style.BRIGHT+"[Sampler.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)



class Sampling:
    def __init__(self, date, db_dir, overide, method, k_neg, k_pos):
        self.date = date
        self.topo = nx.Graph()
        self.topo_irr = nx.Graph()
        self.db_dir = db_dir
        self.overide = overide
        self.method = method
        self.k_neg = k_neg
        self.k_pos = k_pos

        try:
            ut.create_directory("{}/sampling/".format(self.db_dir))
            ut.create_directory("{}/sampling_cluster/".format(self.db_dir))
            ut.create_directory("{}/sampling/negative".format(self.db_dir))
            ut.create_directory("{}/sampling/positive".format(self.db_dir))
            ut.create_directory("{}/sampling/negative/sampling_aspath".format(self.db_dir))
            ut.create_directory("{}/sampling/negative/sampling".format(self.db_dir))
            ut.create_directory("{}/sampling/positive/sampling_aspath_{}".format(self.db_dir, self.method))
            ut.create_directory("{}/sampling/positive/sampling_{}".format(self.db_dir, self.method))
        except FileNotFoundError:
            ut.err_msg("Database directories could not be created, are you sure your database is located at \"{}\" ?".format(self.db_dir))
            exit(1)

    
    def load_topology(self):
        fn_topo = "{}/merged_topology/{}.txt".format(self.db_dir, self.date)
        if os.path.exists(fn_topo):
            print_prefix("Loading topology graph for day {}...".format(self.date))

            start_ts = time()
            self.topo = ut.load_topo_file(fn_topo)
            print_prefix("Topology loaded, nb edges: {}".format(len(self.topo.edges)))

            # Load the suspicious cases during the last 300 days and omit then.
            suspicious_edges = ut.load_suspicious_new_edge(self.db_dir, self.date, 300)
            for as1, as2 in suspicious_edges:
                self.topo.remove_edge(as1, as2)
            print_prefix("Remove suspicious edges, nb edges remaining: {}".format(len(self.topo.edges)))
            stop_ts = time()

            print_prefix("Topology loaded in {:.4f} s ({} links)".format(stop_ts - start_ts, len(self.topo.edges)))

        else:
            ut.err_msg("Unable to find file {}".format(fn_topo))
            exit(1)

        fn_topo = "{}/irr/{}.txt".format(self.db_dir, self.date)
        if os.path.exists(fn_topo):
            print_prefix("Loading IRR topology graph for day {}...".format(self.date))

            start_ts = time()
            self.topo_irr = ut.load_topo_file(fn_topo)
            stop_ts = time()

            print_prefix("IRR loaded in {:.4f} s ({} links)".format(stop_ts - start_ts, len(self.topo_irr.edges)))

        else:
            ut.err_msg("Unable to find file {}".format(fn_topo))

        if len(self.topo.edges()) < 1:
            ut.err_msg("For day {}, topology is empty, abort...".format(self.date))
            exit(1)

    
    def build_negative_sampling(self, size):
        fn_aspaths = "{}/paths/{}-{}-01_paths.txt".format(self.db_dir, self.date.split("-")[0], self.date.split("-")[1])
        fn_sampling = "{}/sampling/negative/sampling/{}_negative.txt".format(self.db_dir, self.date)

        if os.path.exists(fn_sampling) and not self.overide:
            print_prefix("Negative sampling for day {} has been found on local disk, skipped...".format(self.date))
            return

        if os.path.exists(fn_aspaths):
            print_prefix("Building Negative sampling of size {}...".format(size))

            start_ts = time()
            sampling.negative_sampling_forced(self.topo, self.topo_irr, size, self.date, self.db_dir, k=self.k_neg, outfile=fn_sampling, aspath_file=fn_aspaths)
            stop_ts = time()

            print_prefix("Negative sampling for {} has been succefully built in {:.4f} s".format(self.date, stop_ts - start_ts))

        
        else:
            ut.err_msg("Unable to find file {}, skipped...".format(fn_aspaths))
            

    
    def build_positive_sampling(self, size, thresholds):
        fn_aspaths = "{}/paths/{}-{}-01_paths.txt".format(self.db_dir, self.date.split("-")[0], self.date.split("-")[1])
        fn_sampling = "{}/sampling/positive/sampling_{}/{}_positive.txt".format(self.db_dir, self.method, self.date)

        if os.path.exists(fn_sampling) and not self.overide:
            print_prefix("Positive sampling for day {} has been found on local disk, skipped...".format(self.date))
            return

        if os.path.exists(fn_aspaths):
            print_prefix("Building Positive sampling of size {}...".format(size))

            start_ts = time()
            if self.method == "thresholds":
                sampling.positive_sampling_thresholds(self.topo, self.topo_irr, size, k=self.k_pos, outfile=fn_sampling, aspath_file=fn_aspaths, thresholds=thresholds)
            elif self.method == "clusters":
                sampling.positive_sampling_clusters(self.topo, self.topo_irr, size, self.date, self.db_dir, k=self.k_pos, outfile=fn_sampling, aspath_file=fn_aspaths, overide=self.overide)
            elif self.method == "random":
                sampling.positive_sampling_random(self.topo, self.topo_irr, size, outfile=fn_sampling, aspath_file=fn_aspaths)
            else:
                ut.err_msg("Provide a right sampling method")
                exit(1)
            stop_ts = time()

            print_prefix("Positive sampling for {} has been succefully built in {:.4f} s".format(self.date, stop_ts - start_ts))

        
        else:
            ut.err_msg("Unable to find file {}, skipped...".format(fn_aspaths))
            

    
    def build_negative_sampling_aspath(self, size):
        fn_aspaths = "{}/paths/{}-{}-01_paths.txt".format(self.db_dir, self.date.split("-")[0], self.date.split("-")[1])
        fn_sampling = "{}/sampling/negative/sampling_aspath/{}_negative.txt".format(self.db_dir, self.date)

        if os.path.exists(fn_sampling) and not self.overide:
            print_prefix("Negative aspath sampling for day {} has been found on local disk, skipped...".format(self.date))
            return

        if os.path.exists(fn_aspaths):
            print_prefix("Building Negative aspath sampling of size {}...".format(size))

            start_ts = time()
            sampling.negative_sampling_forced(self.topo, self.topo_irr, size, self.date, self.db_dir, k=self.k_neg, outfile=fn_sampling, aspath_file=fn_aspaths)
            stop_ts = time()

            print_prefix("Negative aspath sampling for {} has been succefully built in {:.4f} s".format(self.date, stop_ts - start_ts))

        
        else:
            ut.err_msg("Unable to find file {}, skipped...".format(fn_aspaths))
            

    
    def build_positive_sampling_aspath(self, size, thresholds):
        fn_aspaths = "{}/paths/{}-{}-01_paths.txt".format(self.db_dir, self.date.split("-")[0], self.date.split("-")[1])
        fn_sampling = "{}/sampling/positive/sampling_aspath_{}/{}_positive.txt".format(self.db_dir, self.method, self.date)

        if os.path.exists(fn_sampling) and not self.overide:
            print_prefix("Positive aspath sampling for day {} has been found on local disk, skipped...".format(self.date))
            return

        if os.path.exists(fn_aspaths):
            print_prefix("Building Positive aspath sampling of size {}...".format(size))

            start_ts = time()
            if self.method == "thresholds":
                sampling.positive_sampling_thresholds(self.topo, self.topo_irr, size, k=self.k_pos, outfile=fn_sampling, aspath_file=fn_aspaths, thresholds=thresholds)
            elif self.method == "clusters":
                sampling.positive_sampling_clusters(self.topo, self.topo_irr, size, self.date, self.db_dir, k=self.k_pos, outfile=fn_sampling, aspath_file=fn_aspaths, overide=self.overide)
            elif self.method == "random":
                sampling.positive_sampling_random(self.topo, self.topo_irr, size, outfile=fn_sampling, aspath_file=fn_aspaths)
            else:
                ut.err_msg("Provide a right sampling method")
                exit(1)
            stop_ts = time()

            print_prefix("Positive aspath sampling for {} has been succefully built in {:.4f} s".format(self.date, stop_ts - start_ts))

        
        else:
            ut.err_msg("Unable to find file {}, skipped...".format(fn_aspaths))


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

def sampler_aux(date, db_dir, overide, nb_threads, size, thres, method, k_neg, k_pos):
    sampler = Sampling(date, db_dir, overide, method, k_neg, k_pos)

    sampler.load_topology()
    sampler.build_negative_sampling(size)
    sampler.build_positive_sampling(size, thres)
    sampler.build_negative_sampling_aspath(size)
    sampler.build_positive_sampling_aspath(size, thres)


@click.command()
@click.option("--date", help="Date of the paths we used to build the daily sampling, format YYYY-MM-DD", type=str)
@click.option("--size", default=1000, help="Number of samples for both positive and negative Sampling", type=int)
@click.option("--thresholds", default="0,10,50,100,500,1000,1500,3000,5000,100000", help="Positive smpling thresholds (format thres1,thres2,thres3,thres4)", type=str)
@click.option("--db_dir", default="/tmp/db", help="Database Directory", type=str)
@click.option("--overide", default=0, help="Overide sampling file if already existing", type=int)
@click.option("--nb_threads", default=4, help="Number of threads to build sampling with", type=int)
@click.option("--end_date", default=None, help="End date of bunch", type=str)
@click.option("--method", default="clusters", help="sampling method used", type=str)
@click.option("--k_neg", default=1., help="K parameter used for the negative sampling", type=str)
@click.option("--k_pos", default=1., help="K parameter used for the positive sampling", type=str)


def run_orchestrator(date, size, thresholds, db_dir, overide, nb_threads, end_date, method, k_neg, k_pos):
    k_neg = float(k_neg)
    k_pos = float(k_pos)

    if date is None:
        ut.err_msg("Please enter a date to run this orchestrator")
        exit(1)


    thres = [int(k) for k in thresholds.split(",")]
    
    if end_date is not None:
        all_dates = get_all_dates(date, end_date)
        print(all_dates)
        all_procs = []

        with ProcessPoolExecutor(max_workers=nb_threads) as exec:
            for d in all_dates:
                all_procs.append(exec.submit(sampler_aux, d, db_dir, overide, 1, size, thres, method, k_neg, k_pos))

            for p in all_procs:
                p.result()
        
        exit(0)


    sampler = Sampling(date, db_dir, overide, method, k_pos, k_neg)

    sampler.load_topology()
    sampler.build_negative_sampling(size)
    sampler.build_positive_sampling(size, thres)
    sampler.build_negative_sampling_aspath(size)
    sampler.build_positive_sampling_aspath(size, thres)


if __name__ == "__main__":
    run_orchestrator()