import os
import sys
from datetime import datetime, timedelta
import pandas as pd
from dateutil import relativedelta
from calendar import monthrange
import click 
import psycopg

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.bidirectionality import bidirectional_links
from utils.neighboring_vps import CountNeighboringVPs

class Orchestrator:
    def __init__(self, method, db_dir: str=None):
        self.db_dir = db_dir+'/'
        self.method = method

        if not os.path.isdir(self.db_dir):
            print ('Database does not exist: exit.', file=sys.stderr)
            sys.exit(0)
        if not os.path.isdir(self.db_dir+'/features'):
            os.mkdir(self.db_dir+'/features')
        if not os.path.isdir(self.db_dir+'/features/positive'):
            os.mkdir(self.db_dir+'/features/positive')
        if not os.path.isdir(self.db_dir+'/features/negative'):
            os.mkdir(self.db_dir+'/features/negative')
        if not os.path.isdir(self.db_dir+'/features/positive/bidirectionality_{}'.format(method)):
            os.mkdir(self.db_dir+'/features/positive/bidirectionality_{}'.format(method))
        if not os.path.isdir(self.db_dir+'/features/negative/bidirectionality'):
            os.mkdir(self.db_dir+'/features/negative/bidirectionality')

    def print_prefix(self):
        return Fore.GREEN+Style.BRIGHT+"[Orchestrator.py]: "+Style.NORMAL

    def compute_edge_features_daily_sampling(self, ts: str=None, timespan: int=30, override: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%d")

        # Stop if the outfiles exist already if of override is False.
        outfile_positive = self.db_dir+'/features/positive/bidirectionality_{}/'.format(self.method)+date.strftime("%Y-%m-%d")+"_positive.txt"
        outfile_negative = self.db_dir+'/features/negative/bidirectionality/'+date.strftime("%Y-%m-%d")+"_negative.txt"
        if os.path.isfile(outfile_positive) and os.path.isfile(outfile_negative) and not override:
            return

        sample_file_positive = self.db_dir+'/sampling/positive/sampling_{}/'.format(self.method)+date.strftime("%Y-%m-%d")+"_positive.txt"
        sample_file_negative = self.db_dir+'/sampling/negative/sampling/'+date.strftime("%Y-%m-%d")+"_negative.txt"

        # Parsing the sampled AS links, and creating the dataframe for positive and negative cases.
        positive_links = []
        df_positive = pd.DataFrame(columns=['as1', 'as2'])
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
                df_positive.loc[len(df_positive)] = [int(as1), int(as2)]

        negative_links = []
        df_negative = pd.DataFrame(columns=['as1', 'as2'])
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
                df_negative.loc[len(df_negative)] = [int(as1), int(as2)]  

        print (self.print_prefix()+'Computing bidirectional links for {}.'.format(date))

        # We gather the BGP updates file names for the next 30 days
        bgp_topo_files = []
        irr_topo_files = []
        for i in range(0, timespan+1):
            cur_date = date + timedelta(days=i)

            bgp_topo_file = self.db_dir+'/topology/'+cur_date.strftime("%Y-%m-%d")+"_updates.txt"
            irr_topo_file = self.db_dir+'/irr/'+cur_date.strftime("%Y-%m-%d")+".txt"

            if os.path.isfile(bgp_topo_file) and os.path.isfile(irr_topo_file):
                bgp_topo_files.append(bgp_topo_file)
                irr_topo_files.append(irr_topo_file)

        # We take the first day of the following month for the RIB dump.
        nb_days_cur_month = monthrange(date.year, date.month)[1]
        date_rib = (date + timedelta(days=nb_days_cur_month)).replace(day=1)
        if os.path.isfile(self.db_dir+'/topology/'+date_rib.strftime("%Y-%m-%d")+"_ribs.txt"):
            rib_file = self.db_dir+'/topology/'+date_rib.strftime("%Y-%m-%d")+"_ribs.txt"
            print (self.print_prefix()+'Using the following RIB file: {}.'.format(rib_file))
        else:
            rib_file = None
            print (self.print_prefix()+"RIB not available for: {}".format(date_rib))

        # Merging the resulting dataframes.
        df = bidirectional_links(positive_links, rib_file, bgp_topo_files, irr_topo_files, threshold_days_appearance=1)
        df_positive = df_positive.merge(df, how='left')

        df = bidirectional_links(negative_links, rib_file, bgp_topo_files, irr_topo_files, threshold_days_appearance=1)
        df_negative = df_negative.merge(df, how='left')


        # Initialize object that computes the number of VPs in the neighboring of every AS.
        self.cnvp = CountNeighboringVPs(self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt")

        # Computing and Merging the dataframes about the number of VPs in the neighborhood.
        df = self.cnvp.count_neighboring_vps(positive_links)
        df_positive = df_positive.merge(df, how='left')
        df = self.cnvp.count_neighboring_vps(negative_links)
        df_negative = df_negative.merge(df, how='left')

        # Writing the resulting dataframe.
        df_positive.to_csv(outfile_positive, sep=' ', index=False)
        df_negative.to_csv(outfile_negative, sep=' ', index=False)

    def compute_edge_features_links(self, ts: str, outfile, links=set, timespan: int=30):
        date = datetime.strptime(ts, "%Y-%m-%d")
        print (self.print_prefix()+"Computing edges' features for: {}".format(date), file=sys.stderr)

        df = pd.DataFrame(columns=['as1', 'as2'])

        for as1, as2 in links:
            df.loc[len(df)] = [as1, as2]

        print (self.print_prefix()+'Computing bidirectional links for {}.'.format(date))

        # We gather the BGP updates file names for the next 30 days
        bgp_topo_files = []
        irr_topo_files = []
        for i in range(0, timespan+1):
            cur_date = date + timedelta(days=i)

            bgp_topo_file = self.db_dir+'/topology/'+cur_date.strftime("%Y-%m-%d")+"_updates.txt"
            irr_topo_file = self.db_dir+'/irr/'+cur_date.strftime("%Y-%m-%d")+".txt"

            if os.path.isfile(bgp_topo_file) and os.path.isfile(irr_topo_file):
                bgp_topo_files.append(bgp_topo_file)
                irr_topo_files.append(irr_topo_file)

        # We take the first day of the following month for the RIB dump.
        nb_days_cur_month = monthrange(date.year, date.month)[1]
        date_rib = (date + timedelta(days=nb_days_cur_month)).replace(day=1)
        if os.path.isfile(self.db_dir+'/topology/'+date_rib.strftime("%Y-%m-%d")+"_ribs.txt"):
            rib_file = self.db_dir+'/topology/'+date_rib.strftime("%Y-%m-%d")+"_ribs.txt"
            print (self.print_prefix()+'Using the following RIB file: {}.'.format(rib_file))
        else:
            rib_file = None
            print (self.print_prefix()+"RIB not available for: {}".format(date_rib))

        # Merging the resulting dataframes.
        df_tmp = bidirectional_links(links, rib_file, bgp_topo_files, irr_topo_files, threshold_days_appearance=1)
        df = df.merge(df_tmp, how='left')

        # Initialize object that computes the number of VPs in the neighboring of every AS.
        self.cnvp = CountNeighboringVPs(self.db_dir+'/merged_topology/'+date.strftime("%Y-%m-%d")+".txt")

        # Computing and Merging the dataframes about the number of VPs in the neighborhood.
        df_tmp = self.cnvp.count_neighboring_vps(links)
        df = df.merge(df_tmp, how='left')
        
        # Writing the resulting dataframe.
        if outfile:
            with open(outfile, "w") as f:
                df.to_csv(f, sep=' ', index=False)
        else:
            df.to_csv(sys.stdout, sep=' ', index=False)



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

                if int(as1) > int(as2):
                    as1, as2 = as2, as1

                # Add only the uniq links
                if (as1, as2) not in links:
                    links.add((as1, as2))
    else:
        print("File {} has not been found in the local volume, skipped...".format(link_file), file=sys.stderr)

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
            
            as1 = str(as1)
            as2 = str(as2)
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
@click.option("--outfile", default=None, help="File to print the results", type=str)

def launch_orchestrator(\
    date,
    db_dir,
    override,
    daily_sampling,
    store_results_in_db,
    link_list,
    link_file,
    method,
    outfile):
    """Compute bidirectionality features and store them in the database."""

    if daily_sampling:
        o = Orchestrator(method, db_dir=db_dir)
        o.compute_edge_features_daily_sampling(date, override=override, timespan=30)
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
        o.compute_edge_features_links(date, outfile, links=links, timespan=30)

if __name__ == "__main__":
    launch_orchestrator()
