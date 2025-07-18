import click 
from datetime import date, timedelta
from datetime import datetime
import os.path
import os
from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

class Checker:
    def __init__(self, db_dir: str=None):
        self.db_dir = db_dir

    def print_prefix(self):
        return Fore.GREEN+Style.BRIGHT+"[Checker.py]: "+Style.NORMAL

    def check_topology_database(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/topology/'+date_str+"_updates.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+"/topology/{}_ribs.txt".format(date.replace(day=1).strftime("%Y-%m-%d"))
        if not os.path.isfile(fn):
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_irr_database(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/irr/'+date_str+".txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_paths_database(self, date):
        fn = self.db_dir+"/paths/{}_paths.txt".format(date.replace(day=1).strftime("%Y-%m-%d"))
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_cone_database(self, date):
        fn = self.db_dir+"/cone/{}.txt".format(date.replace(day=1).strftime("%Y-%m-%d"))
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_peeringdb_database(self, date, recover=True):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/peeringdb/'+date_str+"_country.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

            next_date_str = (date + timedelta(days=1)).strftime("%Y-%m-%d")
            next_fn = self.db_dir+'/peeringdb/'+next_date_str+"_country.txt"

            if recover:
                os.system("cp {} {}".format(next_fn, fn))
                print ("cp {} {}".format(next_fn, fn))

        fn = self.db_dir+'/peeringdb/'+date_str+"_ixp.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

            next_date_str = (date + timedelta(days=-1)).strftime("%Y-%m-%d")
            next_fn = self.db_dir+'/peeringdb/'+next_date_str+"_ixp.txt"

            if recover:
                os.system("cp {} {}".format(next_fn, fn))
                print ("cp {} {}".format(next_fn, fn))

        fn = self.db_dir+'/peeringdb/'+date_str+"_facility.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

            print (self.print_prefix()+"Missing {}".format(fn))

            next_date_str = (date + timedelta(days=-1)).strftime("%Y-%m-%d")
            next_fn = self.db_dir+'/peeringdb/'+next_date_str+"_facility.txt"

            if recover:
                os.system("cp {} {}".format(next_fn, fn))
                print ("cp {} {}".format(next_fn, fn))

    def check_newedges(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/merged_topology/'+date_str+".txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/new_edge/'+date_str+".txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_sampling(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/sampling_cluster/'+date_str+".txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/sampling/positive/sampling_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/sampling/positive/sampling_aspath_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/sampling/negative/sampling/'+date_str+"_negative.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/sampling/negative/sampling_aspath/'+date_str+"_negative.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_features(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/features/positive/bidirectionality_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/positive/peeringdb_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/positive/aspath_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/positive/topological_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/negative/bidirectionality/'+date_str+"_negative.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/negative/peeringdb/'+date_str+"_negative.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/negative/aspath/'+date_str+"_negative.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/features/negative/topological/'+date_str+"_negative.txt"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

    def check_aspaths_inference_models(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/aspath_models_clusters/'+date_str+"_model_cone.pkl"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/aspath_models_clusters/'+date_str+"_model_degree.pkl"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))

        fn = self.db_dir+'/aspath_models_clusters/'+date_str+"_model_cone&degree.pkl"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))


    def check_inference_models(self, date):
        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/models/'+date_str+"_model_aspath,bidirectionality,peeringdb,topological.pkl"
        if not os.path.isfile(fn): 
            print (self.print_prefix()+"Missing {}".format(fn))


# Make the CLI.
@click.command()
@click.option('--date_start', help='Start date in the format "YYYY-MM-DD".', type=str)
@click.option('--date_end', help='End date in the format "YYYY-MM-DD".', type=str)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)

def launch_checker(\
    date_start,\
    date_end, \
    db_dir):
    """
    This script that parses a database for a given timeframe 
    and returns the missing files.
    """

    sdate = datetime.strptime(date_start, "%Y-%m-%d")
    edate = datetime.strptime(date_end, "%Y-%m-%d")

    c = Checker(db_dir)
    
    for d in daterange(sdate, edate):
        print ("Checking {}".format(d))
        c.check_topology_database(d)
        c.check_irr_database(d)
        c.check_paths_database(d)
        c.check_cone_database(d)
        c.check_peeringdb_database(d)
        c.check_newedges(d)
        c.check_sampling(d)
        c.check_features(d)
        c.check_aspaths_inference_models(d)
        c.check_inference_models(d)


if __name__ == "__main__":
    launch_checker()