import os
from concurrent import futures
from datetime import datetime

from pybgproutesapi import updates, topology, vantage_points
from utils.cleaning import remove_asprepending

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

def process_vps_chunk(mapping_newedges_to_vps, ixp_set, ts_start, ts_end):
    edge_paths = {e: {} for edges_subset in mapping_newedges_to_vps.values() for e in edges_subset}
    identifier = next(iter(mapping_newedges_to_vps), "N/A")

    print(f"{Fore.YELLOW}[{datetime.now()}] Start collecting aspaths ({identifier}) {Style.RESET_ALL}")
    for vp_ip, edges_subset in mapping_newedges_to_vps.items():
        try:
            vp = vantage_points(vp_ips=vp_ip)  # one call per VP
            topo = topology(vps=vp,
                            date=ts_start.strftime("%Y-%m-%d"),
                            with_aspath=True, with_rib=False, with_updates=True,
                            as_to_ignore=ixp_set, ignore_private_asns=True)
        except Exception as e:
            print(f"{Fore.RED}[{datetime.now()}] Error {e} when retrieving topology for VP {vp_ip} {Style.RESET_ALL}")
            continue

        # Collect this VP's AS paths that touch watched edges
        aspaths = set()
        for aspath_str in topo['aspaths']:
            aspath = [int(x) for x in aspath_str.split()]
            for k in range(len(aspath)-1):
                if ((aspath[k], aspath[k+1]) in edges_subset) or ((aspath[k+1], aspath[k]) in edges_subset):
                    aspaths.add(aspath_str)
                    break

        # Chunk per VP if >20k
        aspaths_list = list(aspaths)
        for i in range(0, len(aspaths_list), 20000):
            chunk = aspaths_list[i:i+20000]
            if not chunk:
                continue
            try:
                data = updates(
                    vps=vp,
                    start_date=ts_start.strftime("%Y-%m-%dT%H:%M:%S"),
                    end_date=ts_end.strftime("%Y-%m-%dT%H:%M:%S"),
                    type_filter='A',
                    return_community=False,
                    return_aspath=True,
                    aspath_exact_match=chunk
                )
            except Exception as e:
                print(f"{Fore.RED}[{datetime.now()}] Error {e} when retrieving updates for VP {vp_ip} {Style.RESET_ALL}")
                continue

            for updates_batch in data.get('bgp', {}).values():
                for upd in updates_batch:
                    aspath_str = upd[3]
                    aspath = remove_asprepending([int(x) for x in aspath_str.split()], ixp_set)
                    if aspath is None:
                        continue
                    aspath_str = ' '.join(map(str, aspath))
                    ts, prefix, vp_asn = int(upd[0]), upd[2], aspath[0]
                    for j in range(len(aspath) - 1):
                        for edge in ((aspath[j], aspath[j+1]), (aspath[j+1], aspath[j])):
                            if edge in edges_subset:
                                cur = edge_paths[edge].get(aspath_str)
                                if cur is None or cur[0] > ts:
                                    edge_paths[edge][aspath_str] = (ts, prefix, vp_ip, vp_asn)

    print(f"{Fore.GREEN}[{datetime.now()}] Finished collecting updates ({identifier}) {Style.RESET_ALL}")
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
        ixp_set = list(self.get_ixps(ixp_file))
        
        # Split the new mapping into {nb_processes} chunks for parallel processing.
        chunk_size = len(mapping_newedges_to_vps) // self.max_workers + 1
        print(f"{Fore.YELLOW}[{datetime.now()}] Splitting mapping_newedges_to_vps ({len(mapping_newedges_to_vps)}) into {self.max_workers} chunks of size {chunk_size}{Style.RESET_ALL}")
        mapping_newedges_to_vps_chunks = [dict(list(mapping_newedges_to_vps.items())[i:i + chunk_size]) for i in range(0, len(mapping_newedges_to_vps), chunk_size)]

        # Initialize a dictionary to hold the edge paths for all vantage points.
        # This will be updated in parallel.
        edge_paths = {e: {} for edges_subset in mapping_newedges_to_vps.values() for e in edges_subset}
        
        # Parallel processing of vantage points to collect prefixes
        with futures.ProcessPoolExecutor(max_workers=len(mapping_newedges_to_vps_chunks)) as executor:
            futures_list = [executor.submit(process_vps_chunk, mapping_newedges_to_vps_chunks[i], ixp_set, ts_start, ts_end) for i in range(len(mapping_newedges_to_vps_chunks))]
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