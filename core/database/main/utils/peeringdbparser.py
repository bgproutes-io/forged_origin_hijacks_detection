import json
import csv
import urllib
import wget
import gzip
from pprint import pprint
import json_lines

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

def print_prefix():
    return Fore.BLUE+Style.BRIGHT+"[peerindbparser.py]: "+Style.NORMAL

def read_asn_facilities(peeringdb_file, outfile):
    print (print_prefix()+"Parsing facilities, saving in: {}".format(outfile))

    net_to_fac = {}  # AS ID -> Fac list.

    # Read the PeeringDB from a CAIDA's snaphot.
    data = json.load(open(peeringdb_file, 'r'))

    # Focus on the netfac item, and get facilities for every AS.
    for netfac in data['netfac']["data"]:
        if netfac["local_asn"] not in net_to_fac:
            net_to_fac[netfac["local_asn"]] = set()
        net_to_fac[netfac["local_asn"]].add((netfac['fac_id'], netfac['name'], netfac['country'], netfac['city']))
    
    # Print the output
    with open(outfile, 'w') as fd:
        for k in sorted(net_to_fac.keys()):
            s = ''
            if len(net_to_fac[k]) > 0:
                for f in net_to_fac[k]:
                    s += "({},\'{}\',\'{}\',\'{}\'),".format(f[0], f[1], f[2], f[3])
                s = s[:-1]

            fd.write('{} {}\n'.format(k, s))

def read_asn_ixps(peeringdb_file, outfile):
    print (print_prefix()+"Parsing ixps, saving in: {}".format(outfile))

    net_to_ixp = {}  # AS ID -> Fac list.
    
    # Read the PeeringDB from a CAIDA's snaphot.
    data = json.load(open(peeringdb_file, 'r'))

    # Focus on the netixlan item, and get ixp for every AS.
    for netixlan in data['netixlan']["data"]:
        if netixlan["asn"] not in net_to_ixp:
            net_to_ixp[netixlan["asn"]] = set()
        net_to_ixp[netixlan["asn"]].add((netixlan['ix_id'], netixlan['name']))
    
    # Print the output
    with open(outfile, 'w') as fd:
        for k in sorted(net_to_ixp.keys()):
            s = ''
            if len(net_to_ixp[k]) > 0:
                for f in net_to_ixp[k]:
                    s += "({},\'{}\'),".format(f[0], f[1])
                s = s[:-1]

            fd.write('{} {}\n'.format(k, s))

def read_ixps(peeringdb_file, caidaixp_tmp_file, outfile):
    print (print_prefix()+"Parsing caida ixps, saving in: {}".format(outfile))

    # Read the IXP info from a CAIDA's files.
    caida_ixp_asn = set()
    with open(caidaixp_tmp_file, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.startswith('#'):
                caida_ixp_asn.add(json.loads(line.rstrip('\n|\r'))['asn'])

    # Read the PeeringDB from a CAIDA's snaphot.
    data = json.load(open(peeringdb_file, 'r'))

    # Get the ASN of all the network with type "route server".
    route_server_asn = set()
    for net in data['net']["data"]:
        if net["info_type"].lower() == "route server" or net["info_type"].lower() == "non-profit":
            route_server_asn.add(net["asn"])

    with open(outfile, 'w') as fd:
        for asn in caida_ixp_asn.intersection(route_server_asn):
            fd.write('{}\n'.format(asn))



def read_asn_country(peeringdb_file, astocountry_file, outfile):
    print (print_prefix()+"Parsing country, saving in: {}".format(outfile))

    asn_to_country = {}

    # Parse the file from CAIDA's as to organization.
    org_to_country = {}
    asn_to_org = {}
    with json_lines.open(astocountry_file.replace('.txt', '.jsonl.gz'), 'r') as fd:
        for item in fd:
            if 'country' in item:
                org_to_country[item['organizationId']] = item['country']
            elif 'asn' in item:
                asn_to_org[item['asn']] = item['organizationId']

    for asn, org_id in asn_to_org.items():
        asn_to_country[int(asn)] = (org_to_country[org_id], 'caida')

    # Parse PeerginDB database.
    org_to_country = {} # Org ID -> country.
    # Read the PeeringDB from a CAIDA's snaphot.
    data = json.load(open(peeringdb_file, 'r'))

    # Get the country for every organisation.
    for org in data['org']["data"]:
        org_to_country[org['id']] = org['country']

    # Get the country for every network, using organzation's country.
    for net in data['net']["data"]:
        if net['org_id'] in org_to_country and len(org_to_country[net['org_id']]) > 0:
            asn_to_country[int(net['asn'])] = (org_to_country[net['org_id']], 'peeringdb')

    # Write the resulting inferred country, with priority given to peeringdb whenever possible.
    with open(outfile, 'w') as fd:
        for asn, value in asn_to_country.items():
            fd.write('{} {} {}\n'.format(asn, value[0], value[1]))





if __name__ == "__main__":
    generate_as_to_country_file('20240101.as-org2info.txt')