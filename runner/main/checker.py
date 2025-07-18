import click 
from datetime import date, timedelta
from datetime import datetime
from dotenv import load_dotenv
import os.path
import os
import psycopg
from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)


# === Load environment variables from .env ===
load_dotenv()

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

class Checker:
    def __init__(self, db_dir: str=None):
        self.db_dir = db_dir

    def print_prefix(self):
        return Fore.RED+Style.BRIGHT+"[Checker.py]: "+Style.NORMAL
    
    def check_db_results_env_variables(self):
        success = True
        if not os.path.exists(".env"):
            print (self.print_prefix()+"Missing .env file.")
            success = False
        
        dfoh_db_name = False
        dfoh_db_user = False
        dfoh_db_password = False
        with open(".env", "r") as f:
            lines  = f.readlines()
            for line in lines:
                line = line.strip()
                if line.startswith("DFOH_DB_NAME="):
                    dfoh_db_name = True
                elif line.startswith("DFOH_DB_USER="):
                    dfoh_db_user = True
                elif line.startswith("DFOH_DB_PWD="):
                    dfoh_db_password = True
        if not (dfoh_db_name and dfoh_db_user and dfoh_db_password):
            print (self.print_prefix()+"Missing DFOH_DB_NAME, DFOH_DB_USER or DFOH_DB_PWD in .env file.")
            success = False
        
        return success

    def check_db_results_connection(self):
        success = True
        db_config = {
            'dbname': os.getenv('DFOH_DB_NAME'),
            'user': os.getenv('DFOH_DB_USER'),
            'password': os.getenv('DFOH_DB_PWD'),
            'host': 'localhost',
            'port': 5432
        }
        try:
            conn = psycopg.connect(**db_config)
        except Exception as e:
            print (self.print_prefix()+"An error occurred while connecting to the database: {}".format(e))
            success = False
        finally:
            if 'conn' in locals():
                conn.close()
        return success
    
    def check_newedges_in_db(self, date, silent=False):
        success = True
        try:
            conn = psycopg.connect(
                dbname=os.getenv('DFOH_DB_NAME'),
                user=os.getenv('DFOH_DB_USER'),
                password=os.getenv('DFOH_DB_PWD'),
                host='localhost',
                port=5432
            )
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM new_link WHERE DATE(observed_at) = %s
                    );
                """, (date.date(),))
                if not cur.fetchone()[0]:
                    if not silent:
                        print(self.print_prefix() + "No new edges found for date {}".format(date))
                    success = False
        except Exception as e:
            if not silent:
                print(self.print_prefix() + "An error occurred: {}".format(e))
            success = False
        finally:
            if 'conn' in locals():
                conn.close()
        
        return success

    def check_topology_database(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")
        success = True

        fn = self.db_dir+'/topology/'+date_str+"_updates.txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+"/topology/{}_ribs.txt".format(date.replace(day=1).strftime("%Y-%m-%d"))
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_irr_database(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")
        success = True

        fn = self.db_dir+'/irr/'+date_str+".txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_paths_database(self, date, silent=False):
        success = True

        fn = self.db_dir+"/paths/{}_paths.txt".format(date.replace(day=1).strftime("%Y-%m-%d"))
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_cone_database(self, date, silent=False):
        success = True

        fn = self.db_dir+"/cone/{}.txt".format(date.replace(day=1).strftime("%Y-%m-%d"))
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success
    
    def check_merged_topology_database(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")
        success = True

        fn = self.db_dir+'/merged_topology/'+date_str+".txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_peeringdb_database(self, date, recover=False, silent=False):
        date_str = date.strftime("%Y-%m-%d")
        success = True

        fn = self.db_dir+'/peeringdb/'+date_str+"_country.txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))

            next_date_str = (date + timedelta(days=1)).strftime("%Y-%m-%d")
            next_fn = self.db_dir+'/peeringdb/'+next_date_str+"_country.txt"
            success = False

            if recover:
                os.system("cp {} {}".format(next_fn, fn))

        fn = self.db_dir+'/peeringdb/'+date_str+"_ixp.txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))

            next_date_str = (date + timedelta(days=-1)).strftime("%Y-%m-%d")
            next_fn = self.db_dir+'/peeringdb/'+next_date_str+"_ixp.txt"
            success = False

            if recover:
                os.system("cp {} {}".format(next_fn, fn))

        fn = self.db_dir+'/peeringdb/'+date_str+"_facility.txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))

            next_date_str = (date + timedelta(days=-1)).strftime("%Y-%m-%d")
            next_fn = self.db_dir+'/peeringdb/'+next_date_str+"_facility.txt"
            success = False

            if recover:
                os.system("cp {} {}".format(next_fn, fn))

        return success

    def check_newedges(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")
        success = True

        fn = self.db_dir+'/new_edge/'+date_str+".txt"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success


    def check_cases(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")
        success = True

        fn = self.db_dir+'/cases/'+date_str+".tmp"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_sampling(self, date, silent=False):
        success = True

        date_str = date.strftime("%Y-%m-%d")

        fn = self.db_dir+'/sampling_cluster/'+date_str+".txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/sampling/positive/sampling_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/sampling/positive/sampling_aspath_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/sampling/negative/sampling/'+date_str+"_negative.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/sampling/negative/sampling_aspath/'+date_str+"_negative.txt"
        if not os.path.isfile(fn):
            if not silent: 
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success
    
    def check_features(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")

        success = True
        fn = self.db_dir+'/features/positive/bidirectionality_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/positive/peeringdb_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/positive/aspath_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/positive/topological_clusters/'+date_str+"_positive.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/negative/bidirectionality/'+date_str+"_negative.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/negative/peeringdb/'+date_str+"_negative.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/negative/aspath/'+date_str+"_negative.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/features/negative/topological/'+date_str+"_negative.txt"
        if not os.path.isfile(fn):
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_aspaths_inference_models(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")

        success = True
        fn = self.db_dir+'/aspath_models_clusters/'+date_str+"_model_cone.pkl"
        if not os.path.isfile(fn):
            if not silent: 
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/aspath_models_clusters/'+date_str+"_model_degree.pkl"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        fn = self.db_dir+'/aspath_models_clusters/'+date_str+"_model_cone&degree.pkl"
        if not os.path.isfile(fn): 
            if not silent:
                print (self.print_prefix()+"Missing {}".format(fn))
            success = False

        return success

    def check_inference_models(self, date, silent=False):
        date_str = date.strftime("%Y-%m-%d")

        success = True
        for suspicious_level in range(1, 11):
            fn = self.db_dir+'/models/'+date_str+"_model_aspath,bidirectionality,peeringdb,topological_{}.pkl".format(suspicious_level)
            if not os.path.isfile(fn):
                if not silent:
                    print (self.print_prefix()+"Missing {}".format(fn))
                success = False

        return success

def find_more_recent_complete_day(db_dir, silent):
    success = False
    cur_day = datetime.today()
    c = Checker(db_dir)

    while not success:
        success = True
        cur_day = cur_day - timedelta(days=1)
        success = c.check_topology_database(cur_day, silent) and success
        success = c.check_irr_database(cur_day, silent) and success
        success = c.check_paths_database(cur_day, silent) and success
        success = c.check_cone_database(cur_day, silent) and success
        success = c.check_peeringdb_database(cur_day, recover=False, silent=silent) and success
        success = c.check_newedges(cur_day, silent) and success
        success = c.check_sampling(cur_day, silent) and success
        success = c.check_features(cur_day, silent) and success
        success = c.check_aspaths_inference_models(cur_day, silent) and success
        success = c.check_inference_models(cur_day, silent) and success
        success = c.check_cases(cur_day, silent) and success

    return cur_day

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
        # print ("Checking {}".format(d))
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
    # find_more_recent_complete_day('/home/holterbach/type1_main/setup/db2')