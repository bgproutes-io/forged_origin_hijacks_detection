import os
import csv
import networkx as nx
from datetime import datetime, timedelta
import click 
import time

import psycopg
from ipaddress import ip_network, ip_address

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.get_paths import GetPath


class NewEdgeFinder:
    def __init__(self, db_dir: str, store_results_in_db: bool, store_results_in_file: bool, max_vps_per_newedge: int, max_workers: int):
        self.db_dir = db_dir
        self.results_db_config = {
            'dbname': os.getenv('DFOH_DB_NAME'),
            'user': os.getenv('DFOH_DB_USER'),
            'password': os.getenv('DFOH_DB_PWD'),
            'host': 'host.docker.internal',
            'port': 5432
        } if store_results_in_db else None
        self.max_vps_per_newedge = max_vps_per_newedge
        self.max_workers = max_workers
        self.prefix_dir = 'new_edge'
        self.store_results_in_file = store_results_in_file

        # Init the database directory for the new edge cases if not created yet.
        if not os.path.isdir(self.db_dir+'/'+self.prefix_dir):
            os.mkdir(self.db_dir+'/'+self.prefix_dir)

    def print_prefix():
        return Fore.WHITE+Style.BRIGHT+"[NewEdgeFinder]: "+Style.NORMAL

    def get_ixp_filename(self, date):
        # Find the ixp list file correponding to the date.
        dateixp = date.replace(day=1)
        month_nb = dateixp.month

        if month_nb == 2 or month_nb == 3:
            month_nb = 1
        elif month_nb == 5 or month_nb == 6:
            month_nb = 4
        elif month_nb == 8 or month_nb == 9:
            month_nb = 7
        elif month_nb == 11 or month_nb == 12:
            month_nb = 10

        dateixp.replace(month=month_nb)
        return self.db_dir+'/peeringdb/'+dateixp.strftime("%Y-%m-%d")+"_ixplist.txt"

    # Helper function to iterate between two dates.
    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def get_vps_subset(self, mapping_vps_to_newedges):
        vps_subset = set()
        tmp_mapping = {}

        # Step 1: Add all VPs for edges seen by <= max_vps_per_newedge VPs
        for newedge, vps in mapping_vps_to_newedges.items():
            if len(vps) <= self.max_vps_per_newedge:
                vps_subset.update(vps)
            else:
                tmp_mapping[newedge] = vps

        # Step 2: Count how many selected VPs already cover each edge and prune fully covered edges
        count_vps_per_newedge = {}
        new_tmp_mapping = {}
        for newedge, vps in tmp_mapping.items():
            new_vps_set = set()
            count_vps_per_newedge[newedge] = 0
            for vp in vps:
                if vp in vps_subset:
                    count_vps_per_newedge[newedge] += 1
                else:
                    new_vps_set.add(vp)

            if count_vps_per_newedge[newedge] < self.max_vps_per_newedge:
                new_tmp_mapping[newedge] = new_vps_set

        count_newedges_per_vp = {}
        for newedge, vps in new_tmp_mapping.items():
            for vp in vps:
                count_newedges_per_vp[vp] = count_newedges_per_vp.get(vp, 0) + 1

        while new_tmp_mapping and count_newedges_per_vp:
            max_vp = max(count_newedges_per_vp, key=count_newedges_per_vp.get)
            vps_subset.add(max_vp)
            del count_newedges_per_vp[max_vp]

            edges_to_remove = []
            for newedge, vps in new_tmp_mapping.items():
                if max_vp in vps:
                    count_vps_per_newedge[newedge] += 1
                    if count_vps_per_newedge[newedge] >= self.max_vps_per_newedge:
                        edges_to_remove.append(newedge)

            for newedge in edges_to_remove:
                del new_tmp_mapping[newedge]

        return vps_subset


    def compute_new_edge(self, datestr: str, nbdays: int):
                        
        date = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S")

        # Get the date for the first day of the X previous month.
        first_day = date - timedelta(days=nbdays)

        # All the suspicious cases detected the last nbdays days (to omit them).
        suspicious_edges = {}
        if self.results_db_config:
            try:
                conn = psycopg.connect(**self.results_db_config)
                with conn.cursor() as cur:
                    # Get all cases in the last nbdays.
                    cur.execute("""
                        SELECT i.asn1, i.asn2, i.classification, MIN(DATE(n.observed_at)) as date
                        FROM new_link n
                        JOIN inference_summary i ON n.inference_id = i.id
                        WHERE DATE(n.observed_at) >= %s AND DATE(n.observed_at) < %s
                        GROUP BY i.asn1, i.asn2, i.classification
                        """, (first_day.strftime("%Y-%m-%d"), date.strftime("%Y-%m-%d")))
                    for as1, as2, classification, observed_at in cur.fetchall():
                        if classification == 'sus':
                            # Store the suspicious edge with the date it was detected.
                            suspicious_edges[(as1, as2)] = observed_at
                        elif classification == 'leg':
                            # Remove the edge if it was previously marked as suspicious.
                            if (as1, as2) in suspicious_edges:
                                del suspicious_edges[(as1, as2)]
                            elif (as2, as1) in suspicious_edges:
                                del suspicious_edges[(as2, as1)]
            except psycopg.Error as e:
                print(NewEdgeFinder.print_prefix() + "Error while querying the database for suspicious edges: {}".format(e))
                print(self.results_db_config)
                return
            finally:
                if conn:
                    conn.close()
        else:
            for cur_date in NewEdgeFinder.daterange(first_day, date):
                case_filename = self.db_dir+'/cases/'+cur_date.strftime("%Y-%m-%d")
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
                                elif (as2, as1) in suspicious_edges:
                                    del suspicious_edges[(as2, as1)]

        print (NewEdgeFinder.print_prefix()+"Number of suspicious edges: {}.".format(len(suspicious_edges)))

        # Load the graph before the date.
        topo_before = nx.Graph()

        filename = self.db_dir+'/merged_topology/'+(date - timedelta(days=1)).strftime("%Y-%m-%d")+".txt"
        if os.path.isfile(filename): 
            with open(filename, 'r') as fd:
                csv_reader = csv.reader(fd, delimiter=' ')
                for row in csv_reader:
                    as1 = int(row[0])
                    as2 = int(row[1])
                    if ((as1, as2) not in suspicious_edges and (as2, as1) not in suspicious_edges) \
                        or ((as1, as2) in suspicious_edges and date.day-suspicious_edges[(as1, as2)].day > 31) \
                        or ((as2, as1) in suspicious_edges and date.day-suspicious_edges[(as2, as1)].day > 31):
                        topo_before.add_edge(as1, as2)
                    else:
                        if (as1, as2) in suspicious_edges:
                            print ('{} {} not added because suspicious {}'.format(as1, as2, suspicious_edges[(as1, as2)]))
                        else:
                            print ('{} {} not added because suspicious {}'.format(as1, as2, suspicious_edges[(as2, as1)]))

        # Check the diff with the edges in the current day to find the new edges.
        filename = self.db_dir+'/topology/'+date.strftime("%Y-%m-%d")+"_updates.txt"
        topo_after = nx.Graph()
        mapping_newedges_to_vps = {}
        new_edges_added = set()
        mapping_vps_to_newedges = {}
        if os.path.isfile(filename): 
            with open(filename, 'r') as fd:
                csv_reader = csv.reader(fd, delimiter=' ')
                for row in csv_reader:
                    # Search for new link
                    # Either the new link did not exist.
                    as1 = int(row[0])
                    as2 = int(row[1])
                    if not topo_before.has_edge(as1, as2):
                        topo_after.add_edge(as1, as2)
                        if len(new_edges_added) <= 200 or (as1, as2) in new_edges_added:
                            if row[2] not in mapping_newedges_to_vps:
                                mapping_newedges_to_vps[row[2]] = set()
                            mapping_newedges_to_vps[row[2]].add((as1, as2))
                            new_edges_added.add((as1, as2))

                            if (as1, as2) not in mapping_vps_to_newedges:
                                mapping_vps_to_newedges[(as1, as2)] = set()
                            mapping_vps_to_newedges[(as1, as2)].add(row[2])

        start = time.time()
        vps_subset = self.get_vps_subset(mapping_vps_to_newedges)
        new_mapping_newedges_to_vps = {}
        for vp, newedges in mapping_newedges_to_vps.items():
            if vp in vps_subset:
                new_mapping_newedges_to_vps[vp] = newedges
        print(f"Time to compute the subset of VPs: {time.time() - start} seconds.")

        print(f"Reduction from {len(mapping_newedges_to_vps)} VPs to {len(new_mapping_newedges_to_vps)} VPs")

        # Get the ixp list filename.
        month_first_day = datetime.strptime(datestr, "%Y-%m-%dT%H:%M:%S").replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        ixp_file = self.get_ixp_filename(month_first_day)
        if not os.path.isfile(ixp_file): 
            print (self.print_prefix()+"IXP File {} not available, continuing without it.".format(ixp_file))

        print (NewEdgeFinder.print_prefix()+datestr+': New edges computed. Found {} new edges.'.format(len(new_edges_added)))

        gp = GetPath(self.max_workers)
        edge_paths = gp.collect_paths(ts_start=date, ts_end=date + timedelta(days=1), ixp_file=ixp_file, mapping_newedges_to_vps=new_mapping_newedges_to_vps)

        # Prepare file and database connections outside the loop
        file_fd = None
        db_cursor = None
        conn = None

        try:
            # Setup file writing if needed
            if self.store_results_in_file:
                filename = self.db_dir + '/' + self.prefix_dir + '/' + date.strftime("%Y-%m-%d") + ".txt"
                file_fd = open(filename, 'w', 1)
                file_fd.write('# Number of edges found: {}\n'.format(topo_after.number_of_edges()))
            
            # Setup database connection if needed
            if self.results_db_config:
                conn = psycopg.connect(**self.results_db_config)
                
                # Remove old entries first (might come from previous runs that failed)
                with conn.cursor() as cur:
                    # Get any timestamp from edge_paths to determine the date
                    sample_edge = next(iter(edge_paths.keys()))
                    sample_aspath = next(iter(edge_paths[sample_edge].keys()))
                    sample_timestamp = edge_paths[sample_edge][sample_aspath][0]
                    cur.execute("""
                        DELETE FROM new_link
                        WHERE DATE(observed_at) = %s
                    """, (datetime.fromtimestamp(int(sample_timestamp)).strftime("%Y-%m-%d"),))
                
                db_cursor = conn.cursor()
            
            # Single loop through edge_paths
            for as1, as2 in edge_paths:
                # Determine if edge is suspicious (computed once per edge)
                past_sus = ((int(as1), int(as2)) in suspicious_edges or 
                        (int(as2), int(as1)) in suspicious_edges)
                
                for aspath in edge_paths[(as1, as2)]:
                    timestamp, prefix, peer_ip, peer_asn = edge_paths[(as1, as2)][aspath]
                    
                    # Write to file if enabled
                    if file_fd:
                        str_tmp = "{}-{}-{}-{}".format(int(timestamp), prefix, peer_ip, peer_asn)
                        file_fd.write("{} {},{},{},{}\n".format(as1, as2, aspath, str_tmp, past_sus))
                    
                    # Insert to database if enabled
                    if db_cursor:
                        db_cursor.execute("""
                            INSERT INTO new_link (
                                asn1, asn2, as_path, observed_at, prefix, peer_ip, peer_asn, is_recurrent, inference_id
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NULL)
                        """, (
                            int(as1), int(as2), aspath, datetime.fromtimestamp(int(timestamp)), 
                            ip_network(prefix), ip_address(peer_ip), int(peer_asn), bool(past_sus)
                        ))
            
            # Commit database changes
            if conn:
                conn.commit()

        except psycopg.Error as e:
            print(NewEdgeFinder.print_prefix() + "Error while inserting new edge in new_link table: {}".format(e))
            if conn:
                conn.rollback()

        finally:
            # Clean up resources
            if file_fd:
                file_fd.close()
            if conn:
                conn.close()


# Make the CLI.
@click.command()
@click.option('--date', help='Date for which to collect the full topology, in the following format "YYYY-MM-DDThh:mm:ss".', type=str)
@click.option('--max_vps_per_newedge', default=10, help='Maximum number of vantage points per new edge to collect AS paths.', type=int)
@click.option('--max_workers', default=1, help='Maximum number of workers when downloading the updates.', type=int)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)
@click.option('--store_results_in_db', default=True, help='If True, store results in the PostgreSQL db.', type=bool)
@click.option('--store_results_in_file', default=False, help='If True, store results in the files.', type=bool)

def compute_new_edge(\
    date, \
    max_vps_per_newedge, \
    max_workers, \
    db_dir, \
    store_results_in_db, \
    store_results_in_file):
    """ Get the new edge links that appear in a given day.
    This script relies on the merged topology.
    If they are not in the database, it builds them first."""

    nef = NewEdgeFinder( \
        db_dir=db_dir, \
        store_results_in_db=store_results_in_db, \
        store_results_in_file=store_results_in_file, \
        max_vps_per_newedge=max_vps_per_newedge, \
        max_workers=max_workers)
    nef.compute_new_edge(date, 300)

if __name__ == "__main__":
    compute_new_edge()