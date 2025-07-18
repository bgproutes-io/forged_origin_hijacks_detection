from concurrent.futures import ProcessPoolExecutor
import os
import sys
from datetime import datetime
import pandas as pd
import click 
from multiprocessing import Process

import psycopg

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.country import CountryFeaturesComputation
from utils.facility import FacilityFeaturesComputation
from utils.ixp import IXPFeaturesComputation
from utils.cosine import CosineDistance
from time import mktime

class Orchestrator:
    def __init__(self, method, db_dir: str=None):
        self.db_dir = db_dir+'/'
        self.method = method

        if not os.path.isdir(self.db_dir):
            print ('Database does not exist: exit.', file=sys.stderr)
            sys.exit(0)
        if not os.path.isdir(self.db_dir+'/features'):
            os.mkdir(self.db_dir+'/features')
        if not os.path.isdir(self.db_dir+'/features/tmp_peeringdb'):
            os.mkdir(self.db_dir+'/features/tmp_peeringdb')
        if not os.path.isdir(self.db_dir+'/features/positive/peeringdb_{}'.format(method)):
            os.mkdir(self.db_dir+'/features/positive/peeringdb_{}'.format(method))
        if not os.path.isdir(self.db_dir+'/features/negative/peeringdb/'):
            os.mkdir(self.db_dir+'/features/negative/peeringdb/')

        self.country_features_obj = None

    def print_prefix(self):
        return Fore.GREEN+Style.BRIGHT+"[Orchestrator.py]: "+Style.NORMAL

    def compute_country_feature_helper(self, topo_file, country_file, outfile_country):
        CountryFeaturesComputation(topo_file, country_file).construct_features(outfile_country)

    def compute_facility_features_helper(self, topo_file, facility_file, outfile_facility_fac, outfile_facility_country, outfile_facility_cities, date):
        ffc = FacilityFeaturesComputation(topo_file, facility_file)
        ffc.construct_features(ffc.node_to_facilities, ffc.mapping_facilities, outfile_facility_fac)
        ffc.construct_features(ffc.node_to_countries, ffc.mapping_countries, outfile_facility_country)
        ffc.construct_features(ffc.node_to_cities, ffc.mapping_cities, outfile_facility_cities)

    def compute_ixp_feature_helper(self, topo_file, ixp_file, outfile_ixp):
        IXPFeaturesComputation(topo_file, ixp_file).construct_features(outfile=outfile_ixp)
        
    def compute_nodes_features(self, ts: str=None, override: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%d")
        topo_file = self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt"
        country_file = self.db_dir+'peeringdb/'+date.strftime("%Y-%m-%d")+"_country.txt"
        facility_file = self.db_dir+'peeringdb/'+date.strftime("%Y-%m-%d")+"_facility.txt"
        ixp_file = self.db_dir+'peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.txt"

        plist = []

        #Compute the country features.
        outfile_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
        if not os.path.isfile(outfile_country) or override: 
            print (self.print_prefix()+"Computing nodes' country features for: {}".format(date), file=sys.stderr)
            self.compute_country_feature_helper(topo_file, country_file, outfile_country)

        # Compute the facility features.
        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"

        if (not os.path.isfile(outfile_facility_fac) or \
            not os.path.isfile(outfile_facility_country) or \
            not os.path.isfile(outfile_facility_cities)) or override: 
            print (self.print_prefix()+"Computing nodes' facility features for: {}".format(date), file=sys.stderr)
            self.compute_facility_features_helper(topo_file, facility_file, outfile_facility_fac, outfile_facility_country, outfile_facility_cities, date)

        # Compute the ixp features.
        outfile_ixp = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
        if not os.path.isfile(outfile_ixp) or override: 
            print (self.print_prefix()+"Computing nodes' ixp features for: {}".format(date), file=sys.stderr)
            self.compute_ixp_feature_helper(topo_file, ixp_file, outfile_ixp)

        # Start the processes.
        # for p in plist:
        #     p.start()

        # # Wait for the processes to terminate.
        # for p in plist:
        #     p.join()

    def compute_edge_features_daily_sampling(self, ts: str=None, override: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%d")
        print (self.print_prefix()+"Computing edges' features for: {}".format(date), file=sys.stderr)

        # Stop if the outfiles exist already if of override is False.
        outfile_positive = self.db_dir+'/features/positive/peeringdb_{}/'.format(self.method)+date.strftime("%Y-%m-%d")+"_positive.txt"
        outfile_negative = self.db_dir+'/features/negative/peeringdb/'+date.strftime("%Y-%m-%d")+"_negative.txt"
        if os.path.isfile(outfile_positive) and os.path.isfile(outfile_negative) and not override:
            return

        topo_file = self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt"
        sample_file_positive = self.db_dir+'/sampling/positive/sampling_{}/'.format(self.method)+date.strftime("%Y-%m-%d")+"_positive.txt"
        sample_file_negative = self.db_dir+'/sampling/negative/sampling/'+date.strftime("%Y-%m-%d")+"_negative.txt"

        # Stop if the sample files are not available.
        if not os.path.isfile(sample_file_positive) or not os.path.isfile(sample_file_negative): 
            print (self.print_prefix()+"Sample files not available. Please do the sampling first.".format(date), file=sys.stderr)
            return 
        
        # Parsing the sampled AS links, and creating the dataframe for positive and negative cases.
        positive_links = []
        self.df_positive = pd.DataFrame(columns=['as1', 'as2'])
        with open(sample_file_positive, 'r') as fd:
            for line in fd.readlines():
                linetab = line.split(',')[0].split(' ')
                if int(linetab[0]) > int(linetab[1]):
                    as1 = linetab[1]
                    as2 = linetab[0]
                else:
                    as1 = linetab[0]
                    as2 = linetab[1]
                positive_links.append((int(as1), int(as2)))
                self.df_positive.loc[len(self.df_positive)] = [int(as1), int(as2)]

        negative_links = []
        self.df_negative = pd.DataFrame(columns=['as1', 'as2'])
        with open(sample_file_negative, 'r') as fd:
            for line in fd.readlines():
                linetab = line.split(',')[0].split(' ')
                if int(linetab[0]) > int(linetab[1]):
                    as1 = linetab[1]
                    as2 = linetab[0]
                else:
                    as1 = linetab[0]
                    as2 = linetab[1]
                negative_links.append((int(as1), int(as2)))
                self.df_negative.loc[len(self.df_negative)] = [int(as1), int(as2)]            

        # Compute cosine distance for country feature.
        country_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
        cd = CosineDistance(topo_file, country_features)
        self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'country_dist'})
        self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'country_dist'})

        # Compute cosine distance for facility features.
        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"

        cd = CosineDistance(topo_file, outfile_facility_fac)
        self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'facility_fac_dist'})
        self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'facility_fac_dist'})

        cd = CosineDistance(topo_file, outfile_facility_country)
        self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'facility_country_dist'})
        self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'facility_country_dist'})

        cd = CosineDistance(topo_file, outfile_facility_cities)
        self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'facility_cities_dist'})
        self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'facility_cities_dist'})

         # Compute cosine distance for country feature.
        ixp_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
        cd = CosineDistance(topo_file, ixp_features)
        self.df_positive = self.df_positive.merge(cd.compute_distance(positive_links), how='left').rename(columns = {'distance':'ixp_dist'})
        self.df_negative = self.df_negative.merge(cd.compute_distance(negative_links), how='left').rename(columns = {'distance':'ixp_dist'})

        # Writing the resulting dataframe.
        self.df_positive.to_csv(outfile_positive, sep=' ', index=False)
        self.df_negative.to_csv(outfile_negative, sep=' ', index=False)


    def compute_edge_features_links(self, ts: str, outfile, links=set):
        date = datetime.strptime(ts, "%Y-%m-%d")
        print (self.print_prefix()+"Computing edges' features for: {}".format(date), file=sys.stderr)

        df = pd.DataFrame(columns=['as1', 'as2'])

        for as1, as2, in links:
            df.loc[len(df)] = [as1, as2]

        topo_file = self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt"

        # Compute cosine distance for country feature.
        country_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
        cd = CosineDistance(topo_file, country_features)
        df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'country_dist'})

        # Compute cosine distance for facility features.
        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"

        cd = CosineDistance(topo_file, outfile_facility_fac)
        df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'facility_fac_dist'})

        cd = CosineDistance(topo_file, outfile_facility_country)
        df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'facility_country_dist'})

        cd = CosineDistance(topo_file, outfile_facility_cities)
        df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'facility_cities_dist'})

        # Compute cosine distance for country feature.
        ixp_features = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
        cd = CosineDistance(topo_file, ixp_features)
        df = df.merge(cd.compute_distance(links), how='left').rename(columns = {'distance':'ixp_dist'})

        # Writing the resulting dataframe in stdout.
        if outfile:
            with open(outfile, "w") as f:
                df.to_csv(f, sep=' ', index=False)
        else:
            df.to_csv(sys.stdout, sep=' ', index=False)

    def clean_files(self, ts: str=None):
        # This function removes all the temporary files (pickle files).
        date = datetime.strptime(ts, "%Y-%m-%d")
        print (self.print_prefix()+"Cleaning up pickle files for: {}".format(date), file=sys.stderr)

        outfile_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_country.pkl"
        if os.path.isfile(outfile_country):
            os.remove(outfile_country) 

        outfile_facility_fac = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_fac.pkl"
        if os.path.isfile(outfile_facility_fac):
            os.remove(outfile_facility_fac)

        outfile_facility_country = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_country.pkl"
        if os.path.isfile(outfile_facility_country):
            os.remove(outfile_facility_country)

        outfile_facility_cities = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_facility_cities.pkl"
        if os.path.isfile(outfile_facility_cities):
            os.remove(outfile_facility_cities)

        outfile_ixp = self.db_dir+'/features/tmp_peeringdb/'+date.strftime("%Y-%m-%d")+"_ixp.pkl"
        if os.path.isfile(outfile_ixp):
            os.remove(outfile_ixp)

def load_link_file(link_file):
    links = set()
    if os.path.exists(link_file):
        with open(link_file, "r") as f:
            for line in f:
                if '#' in line:
                    continue
                
                if "," in line:
                    as1 = int(line.split(",")[0].split(' ')[0])
                    as2 = int(line.split(",")[0].split(' ')[1])
                else:
                    as1 = int(line.split(' ')[0])
                    as2 = int(line.split(' ')[1])

                if as1 > as2:
                    as1, as2 = as2, as1

                # Add only the uniq links
                if (as1, as2) not in links:
                    links.add((as1, as2))

    return links

def get_links_from_db(date):
    links = set()
    conn = psycopg.connect(
        dbname=os.getenv("DFOH_DB_NAME"),
        user=os.getenv("DFOH_DB_USER"),
        password=os.getenv("DFOH_DB_PWD"),
        host="host.docker.internal",
        port=5432
    )
    with conn.cursor() as cur:
        cur.execute("SELECT asn1, asn2 FROM new_link WHERE DATE(observed_at) = %s", (date,))
        rows = cur.fetchall()
        for row in rows:
            as1 = row[0]
            as2 = row[1]

            if as1 > as2:
                as1, as2 = as2, as1

            # Add only the uniq links
            if (as1, as2) not in links:
                links.add((as1, as2))
    conn.close()
    return links

@click.command()
@click.option('--date', help='Date for which to compute peeringdb features, in the following format "YYYY-MM-DD".', type=str)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)
@click.option('--override', default=False, help='Override the existing output files. Default is False.', type=bool)
@click.option("--daily_sampling", default=False, help="Builds the daily sampling, in terms of positive and negative samples. Should be passed with option --date", type=bool)
@click.option('--store_results_in_db', is_flag=True, help='If set, store the results in the PostgreSQL database.')
@click.option("--link_list", default=None, help="List of links to test, in the form \"as1-as2,as3-as4,as5-as6\"", type=str)
@click.option("--link_file", default=None, help="file with the links to read. Each line of the file must be on the form \"as1 as2,whatever you want\" or \"as1 as2 whatever you want\". Basically, these files corresponds to the sampling files", type=str)
@click.option("--method", default="clusters", help="Sampling method used", type=str)
@click.option("--clean", default=True, help="Boolean indicating whether temporary files (which are long to generate) should be cleaned up or not", type=bool)
@click.option("--cache_only", default=False, help="Boolean indicating whether only the nodes' features should be computed and stored for caching", type=bool)
@click.option("--outfile", default=None, help="File to print the results", type=str)

def launch_orchestrator(
    date,
    db_dir,
    override,
    daily_sampling,
    store_results_in_db, 
    link_list,
    link_file, 
    method, 
    clean, 
    cache_only, 
    outfile):
    """Compute peeringDB features and store them in the database."""

    if cache_only:
        o = Orchestrator(method, db_dir=db_dir)
        o.compute_nodes_features(date)
    elif daily_sampling:
        o = Orchestrator(method, db_dir=db_dir)

        outfile_positive = db_dir+'/features/positive/peeringdb_{}/'.format(method)+date+"_positive.txt"
        outfile_negative = db_dir+'/features/negative/peeringdb/'+date+"_negative.txt"

        if os.path.isfile(outfile_positive) and os.path.isfile(outfile_negative) and not override:
            print (o.print_prefix()+"Sampling for day {} already exists, skipped...".format(date), file=sys.stderr)
            return
        
        o.compute_nodes_features(date, override)
        o.compute_edge_features_daily_sampling(date, override)
        o.clean_files(date)
    else:
        links = set()
        if store_results_in_db:
            links = get_links_from_db(date)

        # Add all the links of the option --link_file
        elif link_file:
            links = load_link_file(link_file)

        # Add all the links of the option --link_list
        elif link_list:
            all_l = link_list.split(",")
            for l in all_l:
                as1 = int(l.split("-")[0])
                as2 = int(l.split("-")[1])

                if as1 > as2:
                    as1, as2 = as2, as1
                    
                link = (as1, as2)
                # Add only the uniq links
                if link not in links:
                    links.add(link)
        else:
            print("No links collection option provided. Please provide either --link_list or --link_file or set --store_results_in_db.", file=sys.stderr)
            exit(1)

        o = Orchestrator(method, db_dir=db_dir)
        o.compute_nodes_features(date, override)
        print("We compute node features for date {}".format(date), file=sys.stderr)
        o.compute_edge_features_links(date, outfile, links=links)
        print("We Compute edge features for date {}".format(date), file=sys.stderr)
        if clean:
            o.clean_files(date)
            print("We remove the files for date {}".format(date), file=sys.stderr)
        exit(0)


if __name__ == "__main__":
    launch_orchestrator()
