import wget
import urllib
from datetime import datetime, timedelta

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

def print_prefix():
    return Fore.BLUE+Style.BRIGHT+"[collect_peeringdb.py]: "+Style.NORMAL

def crawl_peeringdb_dump(date_str, outfile):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    found = False

    cur_date = date
    print (print_prefix()+"Finding peeringDB file file...: {}")

    while found == False:
        url = "https://publicdata.caida.org/datasets/peeringdb/{}/{}/peeringdb_2_dump_{}_{}_{}.json".format(cur_date.year, cur_date.strftime('%m'), cur_date.year, cur_date.strftime('%m'), cur_date.strftime('%d'))
        
        try:
            wget.download(url, outfile, bar=None)
            print (print_prefix()+"Successfully Downloaded peeringDB file: {}".format(url))
            found = True

        except urllib.error.HTTPError:
            cur_date = cur_date - timedelta(days=1)

def crawl_ix_asns(date_str, outfile):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    found = False

    cur_date = date
    print (print_prefix()+"Finding CAIDA IXP file...: {}")

    while found == False:
        url = "https://publicdata.caida.org/datasets/ixps/ix-asns_{}{}.jsonl".format(cur_date.year, '%02d' % int(cur_date.strftime('%m')))
        try:
            wget.download(url, outfile, bar=None)
            print (print_prefix()+"Successfully Downloaded CAIDA IXP file: {}".format(url))
            found = True

        except urllib.error.HTTPError:
            cur_date = cur_date - timedelta(days=1)

def crawl_as_org(date_str, outfile):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    found = False

    cur_date = date
    print (print_prefix()+"Finding CAIDA AS-org file...: {}")

    while found == False:
        url = "https://publicdata.caida.org/datasets/as-organizations/{}{}{}.as-org2info.jsonl.gz".format(cur_date.year, cur_date.strftime('%m'), cur_date.strftime('%d'))
        try:
            print (url)
            wget.download(url, outfile, bar=None)
            print (print_prefix()+"Successfully Downloaded CAIDA AS-org file: {}".format(url))
            found = True

        except urllib.error.HTTPError:
            cur_date = cur_date - timedelta(days=1)

if __name__ == "__main__":
    crawl_as_org('2024-04-01', 'tmp.txt')