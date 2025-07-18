import os
import csv
import networkx as nx
from datetime import datetime, timedelta
from concurrent import futures
import click 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class TopoGenerator:
    def __init__(self, db_dir: str, max_workers: str):
        self.db_dir = db_dir
        self.max_workers = max_workers
        self.prefix_dir = 'merged_topology'

        # Init the database directory for the full topology if not created yet.
        if not os.path.isdir(self.db_dir+'/'+self.prefix_dir):
            os.mkdir(self.db_dir+'/'+self.prefix_dir)

    def print_prefix():
        return Fore.WHITE+Style.BRIGHT+"[TopoGenerator]: "+Style.NORMAL


    # Helper function to iterate between two dates.
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    # Function call to generate the topologies for an interval of time (ie multiple days).
    # The function parralellized the generation of the topologies.
    def get_topo_interval(self, \
                            start_date:str, \
                            stop_date:str, \
                            override: bool=False, \
                            nbdays: int=300):

        # Transform string date to datetime object.
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S")
        stop_date = datetime.strptime(stop_date, "%Y-%m-%dT%H:%M:%S")

        # Generating the list of parameters used by the process pool exectur afterwards.
        paramslist = []
        for d in TopoGenerator.daterange(start_date, stop_date):
            cur_d = d.strftime("%Y-%m-%dT%H:%M:%S")
            paramslist.append([self.db_dir, cur_d, override, nbdays, self.prefix_dir])

        with futures.ProcessPoolExecutor(self.max_workers) as executor:
            for result in executor.map(TopoGenerator.get_topo_date, paramslist):
                print (TopoGenerator.print_prefix()+result)

    # Generate the topology for a given day.
    # Takes as input a list of params to be compatible with a call from process pool executor.
    def get_topo_date(paramslist):
        db_dir = paramslist[0]
        datestr = paramslist[1]
        override = paramslist[2]
        nbdays = paramslist[3]
        prefix_dir = paramslist[4]

        print (TopoGenerator.print_prefix()+str(datestr)+': Start to generate the topology')

        # Transform string date to datetime object.
        date = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S")

        # In case the full topology already exists, we just return the corresponding file.
        if os.path.isfile(db_dir+'/'+prefix_dir+'/{}.txt'.format(date.strftime("%Y-%m-%d"))) and not override:
            return "{}: New edge topology already exists.".format(datestr)

        # Get the date for the first day of the X previous month.
        first_day = date - timedelta(days=nbdays)
        last_day = date + timedelta(days=1)

        # first_day = first_day.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Create the topology that will be updated on a daily basis.
        topo_all = nx.Graph()

        for cur_date in TopoGenerator.daterange(first_day, last_day):
            # print (TopoGenerator.print_prefix()+str(cur_date)+ \
            #     ' nb nodes: {} nb edges: {}; all nb nodes: {} all nb edges: {}' \
            #         .format(topo.number_of_nodes(), topo.number_of_edges(), topo_all.number_of_nodes(), topo_all.number_of_edges()))

            # Updates filename.
            updates_filename = db_dir+'/topology/'+cur_date.strftime("%Y-%m-%d")+"_updates.txt"
            
            if os.path.isfile(updates_filename): 
                with open(updates_filename, 'r') as fd:
                    csv_reader = csv.reader(fd, delimiter=' ')
                    for row in csv_reader:
                        topo_all.add_edge(int(row[0]), int(row[1]))
            else: 
                print (TopoGenerator.print_prefix()+datestr+': Update file {} does not exist, skipping it.'.format(updates_filename))

            # Get the RIB filename.
            rib_filename = db_dir+'/topology/'+cur_date.strftime("%Y-%m-%d")+"_ribs.txt"

            if os.path.isfile(rib_filename): 
                # print (TopoGenerator.print_prefix()+'Reading RIB file {}.'.format(rib_filename))

                with open(rib_filename, 'r') as fd:
                    csv_reader = csv.reader(fd, delimiter=' ')
                    for row in csv_reader:
                        topo_all.add_edge(int(row[0]), int(row[1]))

        # Writing the resulting topology (caida + rib from peers + updates) in the database.
        fname = db_dir+'/'+prefix_dir+'/{}.txt'.format(datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d"))
        with open(fname, 'w', 1) as fd:
            for as1, as2 in topo_all.edges():
                fd.write("{} {}\n".format(as1, as2))

        return '{}: Graph built and save'.format(datestr)

# Make the CLI.
@click.command()
@click.option('--date', help='Date for which to collect the full topology, in the following format "YYYY-MM-DDThh:mm:ss".', type=str)
@click.option('--date_end', default=None, help='If this parameter is set, then one topology is computed for every day in between date and date_end".', type=str)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)
@click.option('--nbdays', default=300, help='Number of prior days to consider, default=300.', type=int)
@click.option('--override', default=False, help='Override existing files.', type=bool)
@click.option('--max_workers', default=10, help='Max number of worker when interval of time is used.', type=int)

def generate_topology(\
    date, \
    date_end, \
    db_dir, \
    nbdays, \
    override, \
    max_workers):
    """ Get the full (ie merged) topology from the downloaded updates and rib."""

    tc = TopoGenerator(db_dir, max_workers)
    if date_end is None:
        tc.get_topo_date(date, override, nbdays=nbdays)
    else:
        tc.get_topo_interval(date, \
                            date_end, \
                            override, \
                            nbdays=nbdays)

if __name__ == "__main__":
    generate_topology()