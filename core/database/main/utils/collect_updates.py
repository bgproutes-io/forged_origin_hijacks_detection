import os
from pybgproutesapi import topology, vantage_points

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class CollectUpdates:
    def __init__(self, nb_vps: int=10, max_workers: int=4):

        # Max number of processes when download mrt files.
        self.max_workers = max_workers

        # Get list of vantage points
        self.vps_set = vantage_points(source=["ris", "routeviews", "bgproutes.io", "pch", "cgtf"])

    def print_prefix(self):
        return Fore.YELLOW+Style.BRIGHT+"[collect_updates.py]: "+Style.NORMAL

    # Function to load the ixps number from ixp file.
    def get_ixps(self, infile):
        ixps = set()

        if os.path.isfile(infile):
            with open(infile, 'r') as fd:
                for line in fd.readlines():
                    ixps.add(line.rstrip('\n'))

        return ixps

    def build_snapshot(self, ts_start: str=None, ixp_file: str=None, outfile: str=None):
        date_str = ts_start.strftime("%Y-%m-%d")
        print(self.print_prefix() + '{}: Building snapshot from bgproutes.io'.format(date_str))

        # Load IXP ASN file.
        ases_to_ignore = list(self.get_ixps(ixp_file))

        all_links = set()
        for vp in self.vps_set:
            if not bool(vp['is_active']):
                continue
            try:
                topo = topology([vp['ip']], date_str, with_aspath=False, with_rib=False, with_updates=True, as_to_ignore=ases_to_ignore, ignore_private_asns=True)
                for link in topo['links']:
                    all_links.add((link[0], link[1], vp['ip']))
            except Exception as e:
                print(self.print_prefix() + 'Error retrieving topology for VP {}: {}'.format(vp['ip'], e))

        print(self.print_prefix() + '{}: Topology size: {} links'.format(date_str, len(all_links)))

        if outfile is not None:
            with open(outfile, 'w') as fd:
                for as1, as2, vp in all_links:
                    fd.write("{} {} {}\n".format(as1, as2, vp))

if __name__ == "__main__":
    cu = CollectUpdates(nb_vps=20, max_workers=20)
    cu.build_snapshot(ts_start="2022-05-20T00:00:00", ts_end="2022-05-21T00:00:00", outfile="topo.txt")