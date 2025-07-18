import os
from concurrent import futures
from datetime import datetime

from pybgproutesapi import updates, topology
from utils.cleaning import remove_asprepending

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

def process_vp(mapping_newedges_to_vps, ixp_set, ts_start, ts_end):
    edge_paths = {e: {} for edges_subset in mapping_newedges_to_vps.values() for e in edges_subset}

    for vp_ip, edges_subset in mapping_newedges_to_vps.items():
        # Retrieve all the AS paths containing the new edges seen by the vantage point.
        all_aspaths = set()
        try:
            topo = topology([vp_ip], ts_start.strftime("%Y-%m-%d"), with_aspath=True, with_rib=False, with_updates=True)
        except Exception as e:
            print(f"{Fore.RED}[{datetime.now()}] Error {e} when retrieving topology for Vantage Point {vp_ip}) {Style.RESET_ALL}")
            continue
        for aspath_str in topo['aspaths']:
            aspath = [int(x) for x in aspath_str.split()]
            aspath = remove_asprepending(aspath, ixp_set)
            if aspath is not None:
                for k in range(0, len(aspath)-1):
                    if (aspath[k], aspath[k+1]) in edges_subset:
                        all_aspaths.add(aspath_str)
                    if (aspath[k+1], aspath[k]) in edges_subset:
                        all_aspaths.add(aspath_str)
            else:
                print(f'AS path {aspath} is none in topo for VP {vp_ip}.')

        # Get all the updates with the AS paths
        try:
            vp_updates = updates(
                vp_ip=vp_ip,
                start_date=ts_start.strftime("%Y-%m-%dT%H:%M:%S"),
                end_date=ts_end.strftime("%Y-%m-%dT%H:%M:%S"),
                type_filter='A',
                return_community=False,
                return_aspath=True,
                aspath_exact_match=list(all_aspaths)
            )
        except Exception as e:
            print(f"{Fore.RED}[{datetime.now()}] Error {e} when retrieving updates for Vantage Point {vp_ip}) {Style.RESET_ALL}")
            continue
                    
        for upd in vp_updates:
            aspath_str = upd[3]
            aspath = [int(x) for x in aspath_str.split()]
            aspath = remove_asprepending(aspath, ixp_set)
            if aspath is not None:
                aspath_str = ' '.join(str(x) for x in aspath)
                timestamp = int(upd[0])
                prefix = upd[2]
                vp_asn = aspath[0]
                for j in range(0, len(aspath)-1):
                    if (aspath[j], aspath[j+1]) in edges_subset:
                        if aspath_str not in edge_paths[(aspath[j], aspath[j+1])]:
                            edge_paths[(aspath[j], aspath[j+1])][aspath_str] = (timestamp, prefix, vp_ip, vp_asn)
                        # We only keep the route observed first.
                        elif edge_paths[(aspath[j], aspath[j+1])][aspath_str][0] > timestamp:
                            edge_paths[(aspath[j], aspath[j+1])][aspath_str] = (timestamp, prefix, vp_ip, vp_asn)
                    if (aspath[j+1], aspath[j]) in edges_subset:
                        if aspath_str not in edge_paths[(aspath[j+1], aspath[j])]:
                            edge_paths[(aspath[j+1], aspath[j])][aspath_str] = (timestamp, prefix, vp_ip, vp_asn)
                        # We only keep the route observed first.
                        elif edge_paths[(aspath[j+1], aspath[j])][aspath_str][0] > timestamp:
                            edge_paths[(aspath[j+1], aspath[j])][aspath_str] = (timestamp, prefix, vp_ip, vp_asn)

    return edge_paths

class GetPath:
    def __init__(self, max_workers: int=1):

        # Max number of processes when download mrt files.
        self.max_workers = max_workers

    def print_prefix(self):
        return Fore.WHITE+Style.BRIGHT+"[get_paths.py]: "+Style.NORMAL
    
    # Function to load the ixps number from ixp file.
    def get_ixps(self, infile):
        ixps = set()

        if os.path.isfile(infile):
            with open(infile, 'r') as fd:
                for line in fd.readlines():
                    ixps.add(line.rstrip('\n'))

        return ixps
    
    def collect_paths(self, ts_start: datetime, ts_end: datetime, ixp_file: str, mapping_newedges_to_vps: dict=None):
        # Load IXP ASN file.
        ixp_set = self.get_ixps(ixp_file)
        
        # Split the new mapping into {nb_processes} chunks for parallel processing.
        chunk_size = len(mapping_newedges_to_vps) // self.max_workers + 1
        print(f"{Fore.YELLOW}[{datetime.now()}] Splitting mapping_newedges_to_vps ({len(mapping_newedges_to_vps)}) into {self.max_workers} chunks of size {chunk_size}{Style.RESET_ALL}")
        mapping_newedges_to_vps_chunks = [dict(list(mapping_newedges_to_vps.items())[i:i + chunk_size]) for i in range(0, len(mapping_newedges_to_vps), chunk_size)]

        # Initialize a dictionary to hold the edge paths for all vantage points.
        # This will be updated in parallel.
        edge_paths = {e: {} for edges_subset in mapping_newedges_to_vps.values() for e in edges_subset}
        
        # Parallel processing of vantage points to collect prefixes
        with futures.ProcessPoolExecutor(max_workers=len(mapping_newedges_to_vps_chunks)) as executor:
            futures_list = [executor.submit(process_vp, mapping_newedges_to_vps_chunks[i], ixp_set, ts_start, ts_end) for i in range(len(mapping_newedges_to_vps_chunks))]
            for future in futures_list:
                edge_paths_chunk = future.result()
                for e, paths in edge_paths_chunk.items():
                    if e in edge_paths:
                        edge_paths[e].update(paths)
                    else:
                        edge_paths[e] = paths

        # Filter out edges with no paths
        new_edge_paths = {}
        for e, paths in edge_paths.items():
            if paths:
                new_edge_paths[e] = paths
        print(f"{Fore.GREEN}[{datetime.now()}] {len(new_edge_paths)} new edges with paths over {len(edge_paths)} edges found")
        return new_edge_paths