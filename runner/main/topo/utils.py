import os
import networkx as nx
import csv

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from datetime import date, datetime, timedelta


def print_prefix():
    return Fore.YELLOW+Style.BRIGHT+"[NewEdgeFinder]: "+Style.NORMAL

# Helper function to iterate between two dates.
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def load_topo(db_dir, nbdays=300):
    topo = nx.Graph()

    # Get the date for the first day of the X previous month.
    first_day = date.today() - timedelta(days=nbdays)

    # All the suspicious cases detected the last nbdays days (to omit them).
    suspicious_edges = {}
    for cur_date in daterange(first_day, date.today()):
        case_filename = db_dir+'/cases/'+cur_date.strftime("%Y-%m-%d")
        if os.path.isfile(case_filename):
            with open(case_filename, 'r') as fd:
                for line in fd.readlines():
                    if line.startswith('!sus'):
                        linetab = line.rstrip().split(' ')
                        as1 = int(linetab[1])
                        as2 = int(linetab[2])

                        if (as1, as2) not in suspicious_edges:
                            suspicious_edges[(as1, as2)] = cur_date

                    if line.startswith('!leg'):
                        linetab = line.rstrip().split(' ')
                        as1 = int(linetab[1])
                        as2 = int(linetab[2])
                        if (as1, as2) in suspicious_edges:
                            del suspicious_edges[(as1, as2)]


    delta = 1
    filename = db_dir+'/merged_topology/'+(date.today() - timedelta(days=delta)).strftime("%Y-%m-%d")+".txt"
    
    # Get the more recent merged topology.
    while not os.path.isfile(filename):
        delta += 1
        filename = db_dir+'/merged_topology/'+(date.today() - timedelta(days=delta)).strftime("%Y-%m-%d")+".txt"


    with open(filename, 'r') as fd:
        csv_reader = csv.reader(fd, delimiter=' ')
        for row in csv_reader:
            as1 = int(row[0])
            as2 = int(row[1])
            if ((as1, as2) not in suspicious_edges and (as2, as1) not in suspicious_edges) \
                or ((as1, as2) in suspicious_edges and (date.today()-suspicious_edges[(as1, as2)]).days > 31) \
                or ((as2, as1) in suspicious_edges and (date.today()-suspicious_edges[(as2, as1)]).days > 31):
                topo.add_edge(as1, as2)
            else:
                if (as1, as2) in suspicious_edges:
                    print ('{} {} not added because suspicious {}'.format(as1, as2, suspicious_edges[(as1, as2)]))
                else:
                    print ('{} {} not added because suspicious {}'.format(as1, as2, suspicious_edges[(as2, as1)]))

    print (print_prefix()+"Number of suspicious edges: {}.".format(len(suspicious_edges)))
    print (print_prefix()+"The more recent merged topology found on {}.".format(date.today() - timedelta(days=delta)))


    return topo, set(suspicious_edges.keys())

if __name__ == "__main__":
    topo = load_topo('/home/holterbach/type1_main/setup/db2', 10)