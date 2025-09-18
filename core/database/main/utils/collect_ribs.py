import os
from pybgproutesapi import topology, vantage_points

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)
class CollectRibs:
    def __init__(self, nb_vps: int=20, max_workers: int=10):

        # Max number of processes when download mrt files.
        self.max_workers = max_workers

        # Get list of vantage points with MVP.
        self.vps_set = vantage_points(sources=["ris", "routeviews", "bgproutes.io", "pch", "cgtf"])

    def print_prefix(self):
        return Fore.CYAN+Style.BRIGHT+"[collect_ribs.py]: "+Style.NORMAL

    # Function to load the ixps number from ixp file.
    def get_ixps(self, infile):
        ixps = set()

        if os.path.isfile(infile):
            with open(infile, 'r') as fd:
                for line in fd.readlines():
                    ixps.add(line.rstrip('\n'))

        return ixps

    def build_snapshot(self, date_str: str=None, ixp_file:str=None, outfile: str=None, outfile_paths: str=None, vp_ips_batch_size: int=10):
        print(self.print_prefix() + '{}: Building RIB snapshot from bgproutes.io'.format(date_str))
        
        allpaths = set()
        all_links = set()

        # Load IXP ASN file.
        ases_to_ignore = list(self.get_ixps(ixp_file))

        for i in range(0, len(self.vps_set), vp_ips_batch_size):
            print(f"{self.print_prefix()} Processing batch {i // vp_ips_batch_size + 1} of VPs...")
            batch = self.vps_set[i:i + vp_ips_batch_size]
            batch = [vp for vp in batch if vp.is_active]
            try:
                topo = topology(batch, date_str, with_aspath=True, with_rib=True, with_updates=False, as_to_ignore=ases_to_ignore, ignore_private_asns=True)
                # Normalize links as tuples of integers
                for link in topo["links"]:
                    all_links.add(tuple(link))
                for aspath in topo['aspaths']:
                    allpaths.add(aspath)
            except Exception as e:
                print(f"{self.print_prefix()} Error processing batch {i // vp_ips_batch_size + 1}: {e}")

        print(f"{self.print_prefix()} {date_str}: Topology size: {len(all_links)} links, {len(allpaths)} unique AS paths")
        
        if outfile is not None:
            with open(outfile, 'w') as fd:
                for as1, as2 in all_links:
                    fd.write("{} {}\n".format(as1, as2))
        if outfile_paths is not None:
            with open(outfile_paths, 'w') as fd:
                for path in allpaths:
                    fd.write(path+'\n')

if __name__ == "__main__":
    cr = CollectRibs(max_workers=1)