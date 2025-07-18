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
    print (print_prefix()+"Finding peeringDB file...:")

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
    print (print_prefix()+"Finding CAIDA IXP file...")

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
    print (print_prefix()+"Finding CAIDA AS-org file...")

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

# import wget
# import urllib
# from datetime import datetime

# from colorama import Fore
# from colorama import Style
# from colorama import init
# init(autoreset=True)

# def print_prefix():
#     return Fore.BLUE+Style.BRIGHT+"[collect_peeringdb.py]: "+Style.NORMAL


# def collect_peeringdb(ts, outfile, outfile_caidaixp, astocountry_file):
#     datem = datetime.strptime(ts, "%Y-%m-%d")

#     url = "https://publicdata.caida.org/datasets/peeringdb/{}/{}/peeringdb_2_dump_{}_{}_{}.json".format(datem.year, datem.strftime('%m'), datem.year, datem.strftime('%m'), datem.strftime('%d'))
#     print (print_prefix()+"Download peeringDB file: {}".format(url))

#     datem.replace(day=1)
#     month_nb = int(datem.month)

#     if month_nb == 2 or month_nb == 3:
#         month_nb = 1
#     elif month_nb == 5 or month_nb == 6:
#         month_nb = 4
#     elif month_nb == 8 or month_nb == 9:
#         month_nb = 7
#     elif month_nb == 11 or month_nb == 12:
#         month_nb = 10

#     datem.replace(month=month_nb)
#     url_caida_ixp = "https://publicdata.caida.org/datasets/ixps/ix-asns_{}{}.jsonl".format(datem.year, '%02d' % month_nb)
#     print (print_prefix()+"Download CAIDA IXP file: {}".format(url_caida_ixp))

#     url_asorg = "https://publicdata.caida.org/datasets/as-organizations/{}".format(astocountry_file.split('/')[-1])
#     print (print_prefix()+"Download CAIDA AS-org file: {}".format(url_asorg))

#     try:
#         wget.download(url, outfile, bar=None)
#         wget.download(url_caida_ixp, outfile_caidaixp, bar=None)
#         wget.download(url_asorg, astocountry_file, bar=None)
#         return True
#     except urllib.error.HTTPError:
#         print (print_prefix()+'PeeringDB or CAIDA IXP file not found')
#         return False
    

# if __name__ == "__main__":
#     collect_peeringdb("2019-01-01")