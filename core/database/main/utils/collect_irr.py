import os
from ftplib import FTP
from datetime import datetime
import gzip
import shutil
import bz2
import re 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class CollectIRR:
    def __init__(self, db_dir: str=None):
        self.db_dir = db_dir

        self.radb_ftp = 'ftp.radb.net'
        self.level3_ftp = 'rr.level3.net'

    def print_prefix(self):
        return Fore.MAGENTA+Style.BRIGHT+"[collect_irr.py]: "+Style.NORMAL

    def download_radb_snapshot(self, ts: str, outfile: str):
        date = datetime.strptime(ts, "%Y-%m-%d")

        # RADB date after which file name format has chamged to YYYYMMDD (before YYMMDD).
        date_change = datetime.strptime("2023-11-14", "%Y-%m-%d")

        if date >= date_change:
            date_format = "%Y%m%d"
        else:
            date_format = "%y%m%d"

        ftp = FTP(self.radb_ftp, user='', passwd='')
        ftp.login()

        ftp.cwd('radb/dbase/archive/{}/'.format(date.strftime('%Y')))

        if len(ftp.nlst('*{}*'.format(date.strftime(date_format)))) > 0:
            filename = 'radb.db.{}.gz'.format(date.strftime(date_format))
            with open(self.db_dir+'/tmp/'+filename, 'wb') as fd:
                ftp.retrbinary('RETR radb.db.{}.gz'.format(date.strftime(date_format)), fd.write)
            print (self.print_prefix()+'Done downloading file: {}.'.format(self.db_dir+'/tmp/'+filename))

            with gzip.open(self.db_dir+'/tmp/'+filename, 'rb') as f_in:
                with open(outfile, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.remove(self.db_dir+'/tmp/'+filename)
        
            print (self.print_prefix()+"Temporary IRR RADB files downloaded: {}".format(outfile))

            return [outfile]    
        else:
            print (self.print_prefix()+"RADB IRR snapshot does not exist for that date.")
            return []

        ftp.quit()


    def download_level3_snapshot(self, \
        ts:str, \
        outfile_prefix:str):
        date = datetime.strptime(ts, "%Y-%m-%d")

        # List of the downloaded files
        all_files = []

        # Connect  the Level3 FTP server.
        ftp = FTP(self.level3_ftp, user='', passwd='')
        ftp.login()

        if date.year <= 2020 and date.year >= 2016:
            ftp.cwd('pub/rr/archive/{}'.format(date.year))
            
            for filename in ftp.nlst('*{}*'.format(date.strftime("%y%m%d"))):
                with open(self.db_dir+'/tmp/'+filename, 'wb') as fd:
                    ftp.retrbinary('RETR '+filename, fd.write)
                print (self.print_prefix()+'done downloading file: {}.'.format(self.db_dir+'/tmp/'+filename))

                irr_name = filename.split('.db')[0]

                zipfile = bz2.BZ2File(self.db_dir+'/tmp/'+filename)
                open(outfile_prefix+'_{}.txt'.format(irr_name), 'wb').write(zipfile.read())

                # Store the resulting file name.
                all_files.append(outfile_prefix+'_{}.txt'.format(irr_name))

                # Remove the temporary file.
                os.remove(self.db_dir+'/tmp/'+filename)

        elif date.year > 2020:
            ftp.cwd('pub/rr/archive')

            for filename in ftp.nlst('*{}*'.format(date.strftime("%y%m%d"))):
                with open(self.db_dir+'/tmp/'+filename, 'wb') as fd:
                    ftp.retrbinary('RETR '+filename, fd.write)
                print (self.print_prefix()+'done downloading file: {}.'.format(self.db_dir+'/tmp/'+filename))

                irr_name = filename.split('.db')[0]

                with gzip.open(self.db_dir+'/tmp/'+filename, 'rb') as f_in:
                    with open(outfile_prefix+'_{}.txt'.format(irr_name), 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Store the resulting file name.
                all_files.append(outfile_prefix+'_{}.txt'.format(irr_name))

                # Remove the temporary file.
                os.remove(self.db_dir+'/tmp/'+filename)

        else:
            print (self.print_prefix()+"Level3 IRR snapshot does not exist for that date.")
            return []


        ftp.quit()

        # Get the mirrored IRRs (only available during the last month).
        ftp = FTP(self.level3_ftp, user='', passwd='')
        ftp.login()

        ftp.cwd('pub/rr/archive.mirror-data')
        for filename in ftp.nlst('*{}*'.format(date.strftime("%y%m%d"))):
            irr_name = filename.split('.db')[0]

            if irr_name != 'ripe-nonauth':
                # We omit the RIPE NONAUTH IRR.

                try:
                    print (self.print_prefix()+'start downloading file: {}.'.format(self.db_dir+'/tmp/'+filename))
                    with open(self.db_dir+'/tmp/'+filename, 'wb') as fd:
                        ftp.retrbinary('RETR '+filename, fd.write)
                    print (self.print_prefix()+'done downloading file: {}.'.format(self.db_dir+'/tmp/'+filename))

                    with gzip.open(self.db_dir+'/tmp/'+filename, 'rb') as f_in:
                        with open(outfile_prefix+'_{}.txt'.format(irr_name), 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)

                    # Store the resulting file name.
                    all_files.append(outfile_prefix+'_{}.txt'.format(irr_name))
                    
                    # Remove the temporary file.
                    os.remove(self.db_dir+'/tmp/'+filename)

                except ConnectionResetError:
                    print (self.print_prefix()+'FTP connection down, restarting and continue.')
                    ftp = FTP(self.level3_ftp, user='', passwd='')
                    ftp.login()
                    ftp.cwd('pub/rr/archive.mirror-data')

        ftp.quit()
        return all_files

if __name__ == "__main__":
    cirr = CollectIRR('db/')
    # cirr.download_radb_snapshot("2021-05-20T04:00:00")
    cirr.download_level3_snapshot("2019-05-20T04:00:00")
