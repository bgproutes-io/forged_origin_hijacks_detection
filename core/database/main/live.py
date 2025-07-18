import sys
import click 
import queue
import time 
import os 
import datetime
import multiprocessing 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.collect_live_updates import CollectLiveUpdates

def get_ixp_filename(db_dir):
    cur_day = datetime.datetime.today()

    # Check if IXP file exists for this date.
    i = 0
    while True:
        i += 1
        path = db_dir+'/peeringdb/'+cur_day.strftime("%Y-%m-%d")+"_ixplist.txt"

        if os.path.isfile(path):
            return path
        else:
            cur_day = cur_day - datetime.timedelta(days=1)

        if i == 1000:
            print ('IXP file could not be find after 1000 iterations. Stopping.')
            return None

# Function to load the ixps number from ixp file.
def get_ixps(infile):
    ixps = set()

    if os.path.isfile(infile):
        with open(infile, 'r') as fd:
            for line in fd.readlines():
                ixps.add(line.rstrip('\n'))

    return ixps

def signal_handler(self, sig, frame):
    raise TimeoutError('Collection live took too long. Stop.')

# Make the CLI.
@click.command()
@click.option('--nb_vps', default=10, help='Number of vantage points from which to download updates data .', type=int)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)

def launch_live(nb_vps, db_dir):
    """Collect live data used for hijack detection."""

    while True:
        c = CollectLiveUpdates(nb_vps, db_dir)
        q = multiprocessing.Queue()
        
        # Get the ixp list filename.
        ixp_file = get_ixp_filename(db_dir)
        if ixp_file is None:
            return
        
        # Load IXP ASN file.
        ixp_set = get_ixps(ixp_file)

        # Create two threads that will collect RIS and RV update repsectively.
        p_ris = multiprocessing.Process(target=c.ris_live, args=(q, ixp_set,))
        # thread_rv = threading.Thread(target=c.collect_live_updates_project, args=('routeviews-stream', q, ixp_set,))

        # thread_ris.daemon = True
        # thread_rv.daemon = True

        p_ris.start()
        # thread_rv.start()
        # Read updates in the queue

        while True:
            try:
                str_tmp = q.get_nowait()+'|{}\n'.format(q.qsize())
                # sys.stdout.write(str_tmp)

            except queue.Empty:
                time.sleep(0.1)


if __name__ == "__main__":
    launch_live()