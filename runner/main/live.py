import os
import io
import sys
import click
import time
import networkx as nx
from collections import OrderedDict
from colorama import Fore, Style
from checker import Checker, daterange, find_more_recent_complete_day
from datetime import datetime, timedelta
import subprocess
from multiprocessing import Process, Value

from topo.utils import load_topo
from parse.parse_live import parselive_prefix_to_asns, parselive
from db.utils import create_connection, create_table

def print_prefix():
    return Fore.GREEN+Style.BRIGHT+"[live.py]: "+Style.NORMAL

def run_docker_live(db_dir, docker_image, nb_vps, log_name):
    # kill and remove the docker container.
    subprocess.run("docker kill dfoh_live", shell=True)
    subprocess.run("docker rm dfoh_live", shell=True)

    # Run the docker container.
    docker_cmd = "docker run -itd --name=\"dfoh_live\" \
    --hostname=\"DFOH\" \
    -v {}:/tmp/db \
    {}".format(db_dir, docker_image)
    subprocess.run(docker_cmd, shell=True)

    # Run the live collector.
    cmd = "docker exec -it dfoh_live python3 live.py \
    --db_dir=\"/tmp/db/\" \
    --nb_vps={} \
    --save=\"/tmp/db/live/updates.txt\"".format(nb_vps)

    with open(log_name, 'w') as fd:
        proc = subprocess.Popen(cmd, shell=True, stderr=fd, stdout=subprocess.PIPE)
        for line in io.TextIOWrapper(proc.stdout, encoding="utf-8", newline=''):
            yield (line.rstrip('\n'))

    # kill and remove the docker container.
    subprocess.run("docker kill dfoh_live", shell=True)
    subprocess.run("docker rm dfoh_live", shell=True)

    print (print_prefix()+"DONE: {}".format(log_name))

def create_peeringdb_cache(db_dir, date):
    # kill and remove the docker container.
    subprocess.run("docker kill peeringdb_live_cache", shell=True)
    subprocess.run("docker rm peeringdb_live_cache", shell=True)

    # Run the docker container.
    docker_cmd = "docker run -itd --name=\"peeringdb_live_cache\" \
    --hostname=\"DFOH\" \
    -v {}:/tmp/db \
    unistrahijackdetection/dfoh_peeringdb".format(db_dir)
    subprocess.run(docker_cmd, shell=True)


    args = ["docker", "exec", "-i", "peeringdb_live_cache", "python3", "orchestrator.py", \
        "--db_dir=/tmp/db", \
        "--date={}".format(date), \
        "--cache_only=True"]

    res = subprocess.run(args, stdout=subprocess.PIPE)
    if res.returncode != 0:
        err_msg("Command \"{}\" failed".format(" ".join(args)))
        return None

    # kill and remove the docker container.
    subprocess.run("docker kill peeringdb_live_cache", shell=True)
    subprocess.run("docker rm peeringdb_live_cache", shell=True)

def run_live(db_dir, log_dir, nb_vps, buffer_time, max_workers=10):
    # Create live dir in the database if it does not exist.
    if not os.path.exists(db_dir+'/live'): 
        os.makedirs(db_dir+'/live')

    # Buffer where new links will be stored before launching the inference.
    upd_buffer = OrderedDict()

    # Load the more recent merged topology.
    topo, suspicious_edges = load_topo(db_dir, 300)

    # Shared variable across sub processes that counts the total number of processes.
    nb_processes = Value('i', 0)

    # Shared variables that indicate whether the new link is suspicious or not.
    dic_suspicious_info = {}

    # Generate the peeringDB cache (to speed up inferences).
    data_day = find_more_recent_complete_day(db_dir, silent=True)
    create_peeringdb_cache(db_dir, data_day.strftime("%Y-%m-%d"))
    prefix_to_asns = parselive_prefix_to_asns(db_dir, data_day)

    # Create the database and tables.
    conn = create_connection(db_dir+'/live/db.sql')
    create_table(conn)

    # Iterate over live updates from RIS and RouteViews.
    for line in run_docker_live(db_dir, "unistrahijackdetection/dfoh_db", nb_vps, '{}/live.txt'.format(log_dir)):
        try:
            linetab = line.split('|')
            peer_ip = linetab[1]
            peer_asn = linetab[2]
            ts = float(linetab[3])
            upd_type = linetab[4]

            if upd_type == 'W':
                continue

            prefix = linetab[5]
            aspath = list(map(lambda x:int(x), linetab[6].split(' ')))
        except IndexError:
            # print (print_prefix()+"Could not read update: {}".format(line))
            continue
        
        for i in range(0, len(aspath)-1):
            # For every edge in AS path, if it is not in merged topo:        
            if not topo.has_edge(aspath[i], aspath[i+1]):
                # sys.stdout.write(print_prefix()+'New edge: {} {} aspath: {}\n'.format(aspath[i], aspath[i+1], aspath))
                # If the link was observed in the other direction already: It is legitimate.
                if (aspath[i+1], aspath[i]) in upd_buffer:
                    # We add it into the topology.
                    topo.add_edge(aspath[i+1], aspath[i])
                    # And remove it from the buffer.
                    del upd_buffer[(aspath[i+1], aspath[i])]
                elif (aspath[i+1], aspath[i]) in dic_suspicious_info:
                    # We add it into the topology.
                    topo.add_edge(aspath[i+1], aspath[i])  

                # Insert it in the upd_buffer (or update the upd_buffer)
                else:
                    # If this link is not being inferred now.
                    if (aspath[i], aspath[i+1]) not in dic_suspicious_info and (aspath[i+1], aspath[i]) not in dic_suspicious_info:
                        if (aspath[i], aspath[i+1]) not in upd_buffer:
                            upd_buffer[(aspath[i], aspath[i+1])] = (time.time(), set(), set(), [])
                        upd_buffer[(aspath[i], aspath[i+1])][1].add(tuple(aspath))
                        upd_buffer[(aspath[i], aspath[i+1])][2].add((peer_ip, peer_asn, aspath[-1], prefix, ts, ' '.join(list(map(lambda x:str(x), aspath)))))
                        upd_buffer[(aspath[i], aspath[i+1])][3].append(line)
                        sys.stdout.write(print_prefix()+'Buffer size: {}\n'.format(len(upd_buffer)))

        # Update buffer and launch inference for expired edges.
        while len(upd_buffer) > 0 and time.time() - upd_buffer[next(iter(upd_buffer))][0] > buffer_time:
            new_edge = next(iter(upd_buffer))

            # Create the new link (with as path).
            input_link = "{}-{},".format(new_edge[0], new_edge[1])

            for aspath in upd_buffer[next(iter(upd_buffer))][1]:
                aspath_str = ' '.join(list(map(lambda x:str(x),aspath)))
                input_link += aspath_str+','
            input_link = input_link[:-1]

            # Update the topology only when new links are inferred as legimitate only.
            for as1, as2 in list(dic_suspicious_info.keys()):
                if dic_suspicious_info[(as1, as2)].value == 1:
                    topo.add_edge(as1, as2)
                    del dic_suspicious_info[(as1, as2)]
                    if (as1, as2) in suspicious_edges:
                        suspicious_edges.remove((as1, as2))
                    if (as2, as1) in suspicious_edges:
                        suspicious_edges.remove((as2, as1))
                elif dic_suspicious_info[(as1, as2)].value == 2:
                    del dic_suspicious_info[(as1, as2)]
                    if (as2, as1) not in suspicious_edges:
                        suspicious_edges.add((as1, as2))

            if nb_processes.value < max_workers:
                # Find the more recent days with complete data. 
                data_day_tmp = find_more_recent_complete_day(db_dir, silent=True)
                if data_day_tmp.strftime("%Y-%m-%d") != data_day.strftime("%Y-%m-%d"):
                    data_day = data_day_tmp
                    create_peeringdb_cache(db_dir, data_day.strftime("%Y-%m-%d"))
                    prefix_to_asns = parselive_prefix_to_asns(db_dir, data_day)

                # One shared variable for every new edge.
                # 0: not yet precessed.
                # 1: inferred as legimitate.
                # 2: inferred as suspicious.
                dic_suspicious_info[new_edge] = Value('i', 0)

                # Run the inference in another process.
                p = Process(target=run_broker_inference, args=( \
                    data_day.strftime("%Y-%m-%d"), \
                    db_dir, \
                    upd_buffer[next(iter(upd_buffer))][0], \
                    input_link, \
                    nb_processes, \
                    dic_suspicious_info[new_edge], \
                    prefix_to_asns, \
                    upd_buffer[next(iter(upd_buffer))][2], \
                    upd_buffer[next(iter(upd_buffer))][3], \
                    (new_edge[0], new_edge[1]) in suspicious_edges or (new_edge[1], new_edge[0]) in suspicious_edges))
                p.start()

            else:
                with open('logs/{}_{}_{}_{}.txt'.format(data_day.strftime("%Y-%m-%d"), input_link.split(',')[0].split('-')[0], input_link.split(',')[0].split('-')[1], "inference"), 'w') as fd:
                    fd.write('Could not be processed because not enough workers available.\n')
                    
                    # If a new edge could not be processed, it is considered legitimate and added in the topology.
                    topo.add_edge(new_edge[0], new_edge[1])

            del upd_buffer[new_edge]



def run_broker_inference(date, \
    db_dir, \
    timestamp, \
    input_link, \
    nb_processes, \
    link_inference, \
    prefix_to_asns, \
    newedge_info, \
    raw_updates, \
    is_recurrent):

    nb_processes.value += 1

    # Create connection to database.
    conn = create_connection(db_dir+'/live/db.sql')

    with open('logs_live/{}_{}_{}_{}.txt'.format(date, input_link.split(',')[0].split('-')[0], input_link.split(',')[0].split('-')[1], "inference"), 'w', 1) as fd:
        fd.write("START: {} {} {}\n".format(date, 'inference', input_link))

        for update in raw_updates:
            fd.write(update+'\n')

        # Parameters of the broker
        features = "aspath,bidirectionality,peeringdb,topological"
        nb_days_training=60

        command = "python3 runner/broker.py \
            --date={} \
            --db_dir={} \
            --features={} \
            --input_link=\"{}\" \
            --nb_days_training={} \
            --peeringdb_clean=False"\
            .format(date, \
                    db_dir, \
                    features, \
                    input_link, \
                    nb_days_training)

        inference_str = ''
        proc = subprocess.Popen(command, shell=True, stderr=fd, stdout=subprocess.PIPE)
        for line in io.TextIOWrapper(proc.stdout, encoding="utf-8", newline=''):
            inference_str += line.rstrip('\n')+'\n'
        fd.write(inference_str)

    print (inference_str)
    nb_leg, nb_sus, case_id = parselive(inference_str.rstrip('\n'), timestamp, prefix_to_asns, newedge_info, is_recurrent, conn, 'live.output')
    if nb_leg > 0:
        link_inference.value = 1
    elif nb_sus > 0:
        link_inference.value = 2

    cur = conn.cursor()
    # Insert raw updates in the case_detail table.
    for peer_ip, peer_asn, origin, prefix, ts, aspath in newedge_info:
        sql = ''' INSERT INTO cases_detail(case_id,ts,prefix,aspath,peer_ip,peer_as,peer_name)
        VALUES({},{},"{}","{}","{}","{}","{}") '''.format( \
        case_id, \
        ts, \
        prefix, \
        aspath, \
        peer_ip, \
        peer_asn, \
        'Test')

        cur.execute(sql)
    conn.commit()

    nb_processes.value -= 1


# Make the CLI.
@click.command()
@click.option('--db_dir', default="/root/type1_main/setup/db2", help='Directory where is database.', type=str)
@click.option('--log_dir', default="logs_live", help='Directory where are stored logs.', type=str)
@click.option('--nb_vps', default=10, help='Number of vantage points from which to download updates data .', type=int)
@click.option('--buffer_time', default=5*60, help='The amount of time a new link remains in the buffer before launching the inference (default is 5min)', type=int)
@click.option('--max_workers', default=5, help='Maximum number of parallel workers', type=int)

def launch_live(\
    db_dir, \
    log_dir, \
    nb_vps, \
    buffer_time, \
    max_workers):

    run_live(db_dir, log_dir, nb_vps, buffer_time, max_workers)

if __name__ == "__main__":
    launch_live()