import os
import time
from datetime import datetime, timedelta
import calendar
from multiprocessing import Process
import click 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

from utils.collect_ribs import CollectRibs
from utils.collect_updates import CollectUpdates
from utils.collect_peeringdb import crawl_peeringdb_dump, crawl_ix_asns, crawl_as_org
from utils.collect_irr import CollectIRR
from utils.irrparser import parse_irr_snapshot
from utils.collect_cone import collect_cone_snapshot
from utils.peeringdbparser import read_asn_facilities, read_asn_ixps, read_asn_country, read_ixps
from utils.collect_roas import collect_roas_snapshot

class Orchestrator:
    def __init__(self, db_dir: str=None):

        self.db_dir = db_dir

        # Init the database if not created yet.
        if not os.path.isdir(self.db_dir):
            print ('Database does not exist: creating a new one.')
            os.mkdir(self.db_dir)
        if not os.path.isdir(self.db_dir+'/topology'):
            os.mkdir(self.db_dir+'/topology')
        if not os.path.isdir(self.db_dir+'/irr'):
            os.mkdir(self.db_dir+'/irr')
        if not os.path.isdir(self.db_dir+'/peeringdb'):
            os.mkdir(self.db_dir+'/peeringdb')
        if not os.path.isdir(self.db_dir+'/paths'):
            os.mkdir(self.db_dir+'/paths')
        if not os.path.isdir(self.db_dir+'/cone'):
            os.mkdir(self.db_dir+'/cone')
        if not os.path.isdir(self.db_dir+'/tmp'):
            os.mkdir(self.db_dir+'/tmp')
        if not os.path.isdir(self.db_dir+'/prefixes'):
            os.mkdir(self.db_dir+'/prefixes')

    def print_prefix(self):
        return Fore.GREEN+Style.BRIGHT+"[Orchestrator.py]: "+Style.NORMAL

    def get_ixp_filename(self, date):
        # Find the ixp list file correponding to the date.
        cur_date = date
        while True:
            fn = self.db_dir+'/peeringdb/'+cur_date.strftime("%Y-%m-%d")+"_ixplist.txt"
            if os.path.isfile(fn):
                print (self.print_prefix()+"More recent IXP File used: {}.".format(fn))
                return fn
            cur_date = cur_date - timedelta(days=1)

    def download_timestamp_rib_helper(self, ts: str=None, override: bool=False, nb_vps: int=20, max_workers_rib: int=5):
        month_first_day = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Download the RIB file for the corresponding month.
        rib_file = self.db_dir+'/topology/'+month_first_day.strftime("%Y-%m-%d")+"_ribs.txt"

        # File containing all the AS paths used to build the RIB.
        allpaths_file = self.db_dir+'/paths/'+month_first_day.strftime("%Y-%m-%d")+"_paths.txt"

        # Get the ixp list filename.
        ixp_file = self.get_ixp_filename(month_first_day)
        if not os.path.isfile(ixp_file): 
            print (self.print_prefix()+"IXP File {} not available, continuing without it.".format(ixp_file))

        # Check if the RIB topo is not yet in the DB.
        if not os.path.isfile(rib_file) or not os.path.isfile(allpaths_file) or override: 
            cr = CollectRibs(nb_vps=nb_vps, max_workers=max_workers_rib)
            # cr.build_snapshot(month_first_day.strftime("%Y-%m-%dT%H:%M:%S"), \
            cr.build_snapshot(ts, \
                                             ixp_file=ixp_file, \
                                             outfile=rib_file, \
                                             outfile_paths=allpaths_file)
        else:
            print (self.print_prefix()+"RIB File {} already exists.".format(rib_file))


    def download_timestamp_updates_helper(self, ts: str=None, override: bool=False, max_workers: int=100, nb_vps: int=20):
        cur_day = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(hour=0, minute=0, second=0, microsecond=0)

        update_file = self.db_dir+'/topology/'+cur_day.strftime("%Y-%m-%d")+"_updates.txt"

        # Get the ixp list filename.
        ixp_file = self.get_ixp_filename(cur_day)
        if not os.path.isfile(ixp_file): 
            print (self.print_prefix()+"IXP File {} not available, continuing without it.".format(ixp_file))

        # Check if the updates topo is not yet in the DB.
        if not os.path.isfile(update_file) or override: 
            cu = CollectUpdates(nb_vps=nb_vps, max_workers=max_workers)
            cu.build_snapshot(ts_start=cur_day, ixp_file=ixp_file, outfile=update_file)
        else:
            print (self.print_prefix()+"Update File {} already exists.".format(update_file))

    def download_timestamp_peeringdb(self, ts: str=None, override: bool=False):

        date = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        date_str = date.strftime("%Y-%m-%d")

        peeringdb_tmp_file = self.db_dir+'/tmp/{}_peeringdb.txt'.format(date_str)
        caidaixp_tmp_file = self.db_dir+'/tmp/{}_caidaixp.txt'.format(date_str)
        astocountry_file = self.db_dir+'/tmp/{}_as-org2info.jsonl.gz'.format(date_str)

        peeringdb_file_country = self.db_dir+'/peeringdb/{}_country.txt'.format(date_str)
        peeringdb_file_fac = self.db_dir+'/peeringdb/{}_facility.txt'.format(date_str)
        peeringdb_file_ixp = self.db_dir+'/peeringdb/{}_ixp.txt'.format(date_str)
        peeringdb_file_ixplist = self.db_dir+'/peeringdb/{}_ixplist.txt'.format(date_str)

        if not os.path.isfile(peeringdb_file_country) or \
            not os.path.isfile(peeringdb_file_fac) or \
            not os.path.isfile(peeringdb_file_ixp) or \
            not os.path.isfile(peeringdb_file_ixplist) or \
            override:

            # Download the raw json file from peeringdb. 
            crawl_peeringdb_dump(date_str, peeringdb_tmp_file)
            crawl_ix_asns(date_str, caidaixp_tmp_file)
            crawl_as_org(date_str, astocountry_file)

            # Parse the raw json file and save results in correponding files.
            read_asn_facilities(peeringdb_tmp_file, peeringdb_file_fac)
            read_asn_ixps(peeringdb_tmp_file, peeringdb_file_ixp)
            read_asn_country(peeringdb_tmp_file, astocountry_file, peeringdb_file_country)
            read_ixps(peeringdb_tmp_file, caidaixp_tmp_file, peeringdb_file_ixplist)

            # Remove the raw file.
            os.remove(peeringdb_tmp_file)
            os.remove(caidaixp_tmp_file)
            os.remove(astocountry_file)

        else:
            print (self.print_prefix()+"PeeringDB file {} already exists.".format(peeringdb_tmp_file))

    def download_timestamp_irr(self, ts: str=None, override: bool=False):
        date_str = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").strftime("%Y-%m-%d")
        cirr = CollectIRR(db_dir=self.db_dir+'/')

        if not os.path.isfile(self.db_dir+'/irr/{}.txt'.format(date_str)) or override: 

            # List that where to store the resulting irr files.
            all_irr_files = []

            # Download the raw RADB IRR file.
            irr_file_radb = self.db_dir+'/tmp/{}_irr_radb.txt'.format(date_str)
            all_irr_files.extend(cirr.download_radb_snapshot(date_str, irr_file_radb))
                
            # Download the raw Level3 IRR file, plus more if possible.
            # UPDATE 04/11/2022: It appears that level3 does not publicly share the archive of its IRR :-(
            # irr_file_prefix = self.db_dir+'/tmp/{}_irr'.format(date_str)
            # all_irr_files.extend(cirr.download_level3_snapshot(date_str, irr_file_prefix))


            # Parse the downloaded IRR file and return the inferred topology.
            parse_irr_snapshot(all_irr_files, self.db_dir+'/irr/{}.txt'.format(date_str))
            
            # Remove the raw files.
            for f in all_irr_files:
                os.remove(f)
        else:
            print (self.print_prefix()+"IRR files for date {} already exists.".format(date_str))

    def download_timestamp_cone(self, ts: str=None, override: bool=False):
        # Take the first day of the month,
        date = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S").replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        date_str = date.strftime("%Y-%m-%d")
        
        cone_file = self.db_dir+'/cone/{}.txt'.format(date_str)
        if not os.path.isfile(cone_file) or override: 
            collect_cone_snapshot(date_str, cone_file)
        else:
            print (self.print_prefix()+"Cone file {} already exists.".format(cone_file))

    def download_timestamp_roas(self, ts: str=None, override: bool=False):
        date = datetime.strptime(ts, "%Y-%m-%dT%H:%M:%S")
        year = date.year
        month = f"{date.month:02d}"
        day = f"{date.day:02d}"
        prefixes_file = self.db_dir+'/prefixes/'+date.strftime("%Y-%m-%d")+".txt"

        if not os.path.isfile(prefixes_file) or override:
            collect_roas_snapshot(year, month, day, prefixes_file)
        else:
            print (self.print_prefix()+"Prefixes file {} already exists.".format(prefixes_file))

    def download_timestamp(self, \
        ts: str=None, \
        rib_only: bool=False, \
        updates_only: bool=False, \
        peeringdb_only: bool=False, \
        irr_only: bool=False, \
        cone_only: bool=False, \
        roa_only: bool=False, \
        override_rib: bool=False, \
        override_updates: bool=False, \
        override_peeringdb: bool=False, \
        override_irr: bool=False, \
        override_cone: bool=False, \
        override_roa: bool=False, \
        max_workers_updates: int=100, \
        max_workers_rib: int=5, \
        nb_vps_updates: int=20):

        # First, we need to get peeringDB info, required to process ribs and updates.
        if not rib_only and not updates_only and not irr_only and not cone_only and not roa_only:
            ppeeringdb = Process(target=self.download_timestamp_peeringdb, args=(ts, override_peeringdb,))
            ppeeringdb.start()
            ppeeringdb.join()

        plist = []
        if not rib_only and not updates_only and not peeringdb_only and not cone_only and not roa_only:
            pirr = Process(target=self.download_timestamp_irr, args=(ts, override_irr,))
            plist.append(pirr)

        if not rib_only and not updates_only and not peeringdb_only and not irr_only and not roa_only:
            pcone = Process(target=self.download_timestamp_cone, args=(ts, override_cone,))
            plist.append(pcone)

        if not rib_only and not updates_only and not peeringdb_only and not irr_only and not cone_only:
            proas = Process(target=self.download_timestamp_roas, args=(ts, override_roa))
            plist.append(proas)

        # Start the processes.
        for p in plist:
            p.start()

        # Wait for the processes to terminate.
        for p in plist:
            p.join()

        if not updates_only and not peeringdb_only and not irr_only and not cone_only and not roa_only:
            prib = Process(target=self.download_timestamp_rib_helper, args=(ts, override_rib, nb_vps_updates, max_workers_rib))
            prib.start()
            prib.join()

        if not rib_only and not peeringdb_only and not irr_only and not cone_only and not roa_only:
            pupdates = Process(target=self.download_timestamp_updates_helper, args=(ts, override_updates, max_workers_updates, nb_vps_updates))
            pupdates.start()
            pupdates.join()

# Make the CLI.
@click.command()
@click.option('--date', help='Date for which to collect data, in the following format "YYYY-MM-DDThh:mm:ss".', type=str)
@click.option('--rib_only', default=False, help='Only download the RIB data.', type=bool)
@click.option('--updates_only', default=False, help='Only download the updates data.', type=bool)
@click.option('--peeringdb_only', default=False, help='Only download the PeeringDB data.', type=bool)
@click.option('--irr_only', default=False, help='Only download the IRR data.', type=bool)
@click.option('--cone_only', default=False, help='Only download the data about the customer cone size.', type=bool)
@click.option('--roa_only', default=False, help='Only download the ROA data.', type=bool)
@click.option('--rib_override', default=False, help='Override the existing file in the DB (if any).', type=bool)
@click.option('--updates_override', default=False, help='Override the existing files in the DB (if any).', type=bool)
@click.option('--peeringdb_override', default=False, help='Override the existing file in the DB (if any).', type=bool)
@click.option('--irr_override', default=False, help='Override the existing files in the DB (if any).', type=bool)
@click.option('--cone_override', default=False, help='Override the existing files in the DB (if any).', type=bool)
@click.option('--roa_override', default=False, help='Override the existing files in the DB (if any).', type=bool)
@click.option('--max_workers', default=4, help='Maximum number of workers when downloading the updates.', type=int)
@click.option('--max_workers_rib', default=2, help='Maximum number of workers when downloading the ribs.', type=int)
@click.option('--nb_vps', default=10, help='Number of vantage points from which to download updates data .', type=int)
@click.option('--db_dir', default="db", help='Directory where is database.', type=str)

def launch_orchestrator(\
    date,\
    rib_only,\
    updates_only,\
    peeringdb_only,\
    irr_only,\
    cone_only,\
    roa_only, \
    rib_override,\
    updates_override,\
    peeringdb_override,\
    irr_override,\
    cone_override,\
    roa_override,\
    max_workers,\
    max_workers_rib,\
    nb_vps,\
    db_dir):
    """Collect raw data used for hijack detection and store it in a database."""

    o = Orchestrator(db_dir)
    o.download_timestamp(\
        ts=date, \
        rib_only=rib_only, \
        updates_only=updates_only, \
        peeringdb_only=peeringdb_only, \
        irr_only=irr_only, \
        cone_only=cone_only, \
        roa_only=roa_only, \
        override_rib=rib_override, \
        override_updates=updates_override, \
        override_peeringdb=peeringdb_override, \
        override_irr=irr_override, \
        override_cone=cone_override, \
        override_roa=roa_override, \
        max_workers_updates=max_workers, \
        max_workers_rib=max_workers_rib, \
        nb_vps_updates=nb_vps)

if __name__ == "__main__":
    launch_orchestrator()


    # o.download_timestamp("2022-08-05T04:00:00", irr_only=True, override_irr=True, max_workers_updates=100, nb_vps_updates=10)

    # fd = open('exection_time_2.txt', 'w', 1)

    # for nb_vps in [10, 40, 100, 150, 200, 300, 400, 500]:
    #     st = time.time()
    #     o.download_timestamp("2021-05-20T04:00:00", override_updates=True, updates_only=True, max_workers_updates=100, nb_vps_updates=nb_vps)
    #     et = time.time()
    #     fd.write('Execution time: {} seconds with {} vps\n'.format(et-st, nb_vps))

    # fd.close()