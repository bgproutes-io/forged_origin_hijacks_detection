import os
import csv
import networkx as nx
from datetime import datetime, timedelta
import click 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class TopoParser:
    def __init__(self, db_dir: str):
        self.db_dir = db_dir

        # Init the database directory for the full topology if not created yet.
        if not os.path.isdir(self.db_dir+'/full_topology'):
            os.mkdir(self.db_dir+'/full_topology')

    def print_prefix(self):
        return Fore.WHITE+Style.BRIGHT+"[get_topology.py]: "+Style.NORMAL

    def get_topo_date(self, datestr: str, override: bool=False):

        # Transform string date to datetime object.
        date = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S")

        # In case the full topology already exists, we just return the corresponding file.
        if os.path.isfile(self.db_dir+'/full_topology/{}_full.txt'.format(date.strftime("%Y-%m-%d"))) and not override:
            print (self.print_prefix()+"Full topology for {} already exists.".format(date))
            return self.db_dir+'/full_topology/{}_full.txt'.format(date.strftime("%Y-%m-%d"))

        # In case it does not exist yet, we build it.

        # Get the date for the first day of the previous month.
        last_day_of_prev_month = date.replace(day=1) - timedelta(days=1)
        start_day_of_prev_month = date.replace(day=1) - timedelta(days=last_day_of_prev_month.day)
        
        # Helper function to iterate between two dates.
        def daterange(start_date, end_date):
            for n in range(int((end_date - start_date).days)):
                yield start_date + timedelta(n)

        # Retrieve the name of the topo files around the given date.
        filename_list = []
        for cur_date in daterange(start_day_of_prev_month, date):
            filename_list.append(self.db_dir+'/topology/'+cur_date.strftime("%Y-%m-%d")+"_updates.txt")

        topo = nx.DiGraph()

        # Building the one-month graph snaphot.
        for filename in filename_list:
            print (self.print_prefix()+"Merging update file: {}".format(filename))
            if os.path.isfile(filename): 
                with open(filename, 'r') as fd:
                    csv_reader = csv.reader(fd, delimiter=' ')
                    for row in csv_reader:
                        topo.add_edge(int(row[0]), int(row[1]))
            else: 
                print (self.print_prefix()+'Update file {} does not exist, skipping it.'.format(filename))

        # Merging it with the RIB graph of the same month.
        rib_file = self.db_dir+'/topology/'+start_day_of_prev_month.strftime("%Y-%m-%d")+"_ribs.txt"
        print (self.print_prefix()+"Merging RIB file: {}".format(rib_file))

        if os.path.isfile(rib_file): 
            with open(rib_file, 'r') as fd:
                csv_reader = csv.reader(fd, delimiter=' ')
                for row in csv_reader:
                    topo.add_edge(int(row[0]), int(row[1]))
        else:
            print (self.print_prefix()+'RIB file {} does not exist, skipping it.'.format(filename))

        # Writing the resulting topology in the cache.
        fname =self.db_dir+'/full_topology/{}_full.txt'.format(datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d"))
        print (self.print_prefix()+"Writing full topology in: {}".format(fname))
        with open(fname, 'w', 1) as fd:
            for as1, as2 in topo.edges():
                fd.write("{} {}\n".format(as1, as2))
            
        print (self.print_prefix()+'Topo size: {} {}'.format(datestr, topo.number_of_nodes(), topo.number_of_edges()))

# Make the CLI.
@click.command()
@click.option('--date', help='Date for which to collect the full topology, in the following format "YYYY-MM-DDThh:mm:ss".', type=str)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)
@click.option('--override', default=False, help='Override existing files.', type=bool)

def get_topology(\
    date, \
    db_dir, \
    override):
    """ Get the full topology from the downloaded updates and rib."""

    tc = TopoParser(db_dir)
    tc.get_topo_date(date, override)

if __name__ == "__main__":
    get_topology()