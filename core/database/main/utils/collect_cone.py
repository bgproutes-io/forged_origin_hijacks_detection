import sys
import json
import time
from graphqlclient import GraphQLClient
import datetime

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

# Part of this script was inspired by https://api.asrank.caida.org/v2/scripts/asrank-download.py


def print_prefix():
    return Fore.RED+Style.BRIGHT+"[collect_cone.py]: "+Style.NORMAL


######################################################################
## Walks the list until it is empty
######################################################################
def DownloadList(date, debug_limit: int=0):
    URL = "https://api.asrank.caida.org/v2/graphql"

    PAGE_SIZE = 10000

    decoder = json.JSONDecoder()
    encoder = json.JSONEncoder()    
    hasNextPage = True
    offset = 0

    # Used by nested calls
    start = time.time()
    while hasNextPage:
        type,query = AsnsQuery(PAGE_SIZE, offset, date)

        # Download the data in this page.
        client = GraphQLClient(URL)
        data = decoder.decode(client.execute(query))
        
        if not ("data" in data and type in data["data"]):
            print (print_prefix()+"Failed to parse:",data,file=sys.stderr)
            sys.exit()

        # Process the asns.
        data = data["data"][type]
        for node in data["edges"]:
            asn = node['node']['asn']
            conesize = node['node']['cone']['numberAsns']
            yield (asn, conesize)

        # Move to the next page.
        hasNextPage = data["pageInfo"]["hasNextPage"]
        offset += data["pageInfo"]["first"]

        print (print_prefix()+"    ",offset,"of",data["totalCount"], " ",time.time()-start,"(sec)",file=sys.stderr)
        start = time.time()

        # Stop early for debugging, if activated.
        if debug_limit and debug_limit < offset:
            hasNextPage = False

######################################################################
## Queries
######################################################################

def AsnsQuery(first,offset, date):
    start_date = date
    end_date = (datetime.datetime.strptime(date, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # start_date="2021-10-01"
    # end_date="2021-10-03"

    return [
        "asns", 
        """{
        asns(first:%s, offset:%s, dateStart:"%s", dateEnd:"%s") {
            totalCount
            pageInfo {
                first
                hasNextPage
            }
            edges {
                node {
                    asn
                    asnName
                    rank
                    date
                    cone {
                        numberAsns
                        numberPrefixes
                        numberAddresses
                    }
                    asnDegree {
                        provider
                        peer
                        customer
                        total
                        transit
                        sibling
                    }
                }
            }
        }
    }""" % (first, offset, start_date, end_date)
    ]

def collect_cone_snapshot(date:str, outfile: str, debug_limit: int=False):

    l = []
    for asn, conesize in DownloadList(date, debug_limit=debug_limit):
        l.append((asn, conesize))

    with open(outfile, 'w') as fd:
        for asn, conesize in l:
                fd.write("{} {}\n".format(asn, conesize))


if __name__ == "__main__":
    collect_cone_snapshot("2021-01-01", "asns.json")