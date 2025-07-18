import networkx as nx
from datetime import datetime, timedelta
# import pybgpstream
import time
import os 
import sys
import signal
import random 
import json
import websocket
import multiprocessing 
from contextlib import redirect_stderr

from utils.mvp import get_vps
from utils.vps import get_vps_info
from utils.cleaning import remove_asprepending

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class CollectLiveUpdates:
    def __init__(self, nb_vps: int=10, db_dir:str=None):
        # Get list of vantage points with MVP.
        self.nb_vps = nb_vps
        self.vps_info = get_vps_info()
        vps_set = get_vps(nb_vps)
        self.db_dir = db_dir

        # Retrieve a list of collectors from which to download data
        self.collectors = set()
        for vp in vps_set:
            self.collectors.add(vp[0])
        
        # From the VP's asn, retrieve the corresponding peer's IPs.
        self.peers = set()

        for collector, asn in vps_set:
            if asn in self.vps_info:
                for vp in self.vps_info[asn]:
                    if vp[0].lower() == collector.lower():
                        self.peers.add(vp[2])

    def print_prefix(self):
        return Fore.YELLOW+Style.BRIGHT+"[collect_updates.py]: "+Style.NORMAL

    def ris_live(self, q, ixp_set):
        ws = websocket.WebSocket()
        ws.connect("wss://ris-live.ripe.net/v1/ws/?client=py-example-1")
        params = {
            "type":"UPDATE",
            "socketOptions": {
                "includeRaw": False
            }
        }
        ws.send(json.dumps({
                "type": "ris_subscribe",
                "data": params
        }))

        for data in ws:
            try:
                parsed = json.loads(data)
            except json.decoder.JSONDecodeError:
                sys.stderr.write(str(data)+'\n')
                continue

            if parsed['type'] == 'ris_message':
                if parsed['data']['peer'] in self.peers:
                    
                    aspath_list = []

                    if len(parsed['data']["path"]) > 0:
                        if isinstance(parsed['data']["path"][-1], list):
                            for origin in parsed['data']["path"][-1]:
                                aspath = parsed['data']["path"][:-1]
                                aspath.append(origin)
                                # Clean up the AS path.
                                print (aspath)
                                aspath = " ".join(str(element) for element in remove_asprepending(aspath, ixp_set))
                                aspath_list.append(aspath)
                                print (parsed['data']["path"], aspath_list)
                        else:
                            aspath = " ".join(str(element) for element in remove_asprepending(parsed['data']["path"], ixp_set))
                            aspath_list = [aspath] 

                    for aspath in aspath_list:
                        for dic_nh_prefix in parsed['data']['announcements']:
                            for prefix in dic_nh_prefix['prefixes']:
                                s = '{}|{}|{}|{}|{}|{}|{}'.format(
                                    'ris-live',
                                    parsed['data']["peer"],
                                    parsed['data']["peer_asn"],
                                    time.time(),
                                    'U',
                                    prefix,
                                    aspath)
                                q.put(s)

                    for prefix in parsed['data']['withdrawals']:
                        s = ('{}|{}|{}|{}|{}|{}'.format(
                            'ris-live',
                            parsed['data']["peer"],
                            parsed['data']["peer_asn"],
                            time.time(),
                            'W',
                            prefix))
                        q.put(s)




if __name__ == "__main__":
    cu = CollectLiveUpdates(nb_vps=10)
    q = multiprocessing.Queue()
