import os
import sys
import click 
import time
from datetime import datetime
from colorama import Fore, Style
from checker import Checker, daterange
from datetime import datetime, timedelta
import subprocess
import schedule

# import runner.broker as br
from accuracy.orchestration import accuracy, clusters
from parse.parse import launch_parser


def print_prefix():
    return Fore.GREEN+Style.BRIGHT+"[run_daily.py]: "+Style.NORMAL



def run_cmd(db_dir, docker_image, cmds, date, log_name):
    # kill and remove the docker container.
    subprocess.run("docker kill dfoh_rundaily", shell=True)
    subprocess.run("docker rm dfoh_rundaily", shell=True)

    # Run the docker container.
    docker_cmd = "docker run -itd --name=\"dfoh_rundaily\" \
    --hostname=\"dfoh\" \
    --add-host=host.docker.internal:host-gateway \
    --env-file .env \
    -v {}:/tmp/db \
    {}".format(db_dir, docker_image)
    subprocess.run(docker_cmd, shell=True)

    # Run the command.
    with open('logs/{}_{}.txt'.format(date.strftime("%Y-%m-%d"), log_name), 'w') as fd:
        for cmd in cmds:
            subprocess.run(cmd, shell=True, stdout=fd, stderr=fd)

    # kill and remove the docker container.
    subprocess.run("docker kill dfoh_rundaily", shell=True)
    subprocess.run("docker rm dfoh_rundaily", shell=True)


    print (print_prefix()+"DONE: {} {}".format(date, log_name))


def run_database(date :str, db_dir):
    print (print_prefix()+"START: {} {}".format(date, 'database'))
    c = Checker(db_dir)
    # First check whether all the environment variables are set.
    success = True
    success = c.check_db_results_env_variables() and success

    if success:
    # Collect the data base.
        command = "docker exec -it dfoh_rundaily python3 collector.py \
            --date={}T00:00:00 \
            --nb_vps=200 \
            --max_workers=5 \
            --max_workers_rib=2 \
            --db_dir=\"/tmp/db/\"".format(date.strftime("%Y-%m-%d"))

        run_cmd(db_dir, "unistrahijackdetection/dfoh_db", [command], date, 'database')

    print (print_prefix()+"START: {} {}".format(date, 'merger'))
    # Compute the merged topology.

    sdate = date + timedelta(days=-300)
    edate = date + timedelta(days=1)

    # Check whether all the data is there.
    success = True
    for d in daterange(sdate, edate):
        success = c.check_topology_database(d) and success

    if success:
        command = "docker exec -it dfoh_rundaily python3 merger.py \
            --date={}T00:00:00 \
            --date_end={}T00:00:00 \
            --db_dir=\"/tmp/db/\" \
            --nbdays=300 \
            --max_workers=10 \
            --override=0".format(date.strftime("%Y-%m-%d"), (date + timedelta(days=1)).strftime("%Y-%m-%d"))
        
        run_cmd(db_dir, "unistrahijackdetection/dfoh_newedge", [command], date, 'merger')



def run_sampling(date :str, db_dir):
    print (print_prefix()+"START: {} {}".format(date, 'sampling'))

    # First check whether all the data is there.
    c = Checker(db_dir)

    success = True
    success = c.check_topology_database(date) and success
    success = c.check_irr_database(date) and success
    success = c.check_paths_database(date) and success
    success = c.check_merged_topology_database(date) and success


    if success:
        command = "docker exec -it dfoh_rundaily python3 sampler.py \
            --date={} \
            --db_dir=\"/tmp/db/\" \
            --overide=1 \
            --nb_threads=10 \
            --size=1000 \
            --k_pos=0.75 \
            --k_neg=1".format(date.strftime("%Y-%m-%d"))

        run_cmd(db_dir, "unistrahijackdetection/dfoh_sampling", [command], date, 'sampling')

def run_features(date :str, db_dir):
    print (print_prefix()+"START: {} {}".format(date, 'features'))

    # First check whether all the data is there.
    c = Checker(db_dir)

    success = True
    sdate = date + timedelta(days=-60)
    edate = date + timedelta(days=1)
    # We need 60 days before the AS path pattern features need its own model.
    for d in daterange(sdate, edate):
        success = c.check_topology_database(d) and success
        success = c.check_irr_database(d) and success
        success = c.check_paths_database(d) and success
        success = c.check_merged_topology_database(d) and success    
        success = c.check_cone_database(d) and success
        success = c.check_peeringdb_database(d) and success
        success = c.check_sampling(d) and success

    if success:
        # Run the AS-path features
        command = "docker exec -it dfoh_rundaily python3 aspath_feat.py \
            --date={} \
            --db_dir=\"/tmp/db/\" \
            --daily_sampling 1 \
            --nbdays=300 \
            --override=1".format(date.strftime("%Y-%m-%d"))
        run_cmd(db_dir, "unistrahijackdetection/dfoh_aspathfeat", [command], date, 'aspathfeat')

        # Run Bidirectionnality features
        command = "docker exec -it dfoh_rundaily python3 orchestrator.py \
            --date={} \
            --db_dir=\"/tmp/db/\" \
            --daily_sampling=True \
            --method=clusters \
            --override=1".format(date.strftime("%Y-%m-%d"))
        run_cmd(db_dir, "unistrahijackdetection/dfoh_bidirectionality", [command], date, 'bidirectionality')

        # Run PeeringDB features
        command = "docker exec -it dfoh_rundaily python3 orchestrator.py \
            --date={} \
            --db_dir=\"/tmp/db/\" \
            --daily_sampling=True \
            --method=clusters \
            --override=True".format(date.strftime("%Y-%m-%d"))
        run_cmd(db_dir, "unistrahijackdetection/dfoh_peeringdb", [command], date, 'peeringdb')

        # Run Topological features
        command = "docker exec -it dfoh_rundaily python3 topo_feat.py \
            --date={} \
            --db_dir=\"/tmp/db/\" \
            --daily_sampling 1 \
            --overide=1 \
            --nb_threads=10".format(date.strftime("%Y-%m-%d"))
        run_cmd(db_dir, "unistrahijackdetection/dfoh_topological", [command], date, 'topological')

def run_new_edges(date, db_dir, store_results_in_db, store_results_in_file):
    print (print_prefix()+"START: {} {}".format(date, 'new_edge'))

    # First check whether all the data is there.
    c = Checker(db_dir)

    success = True
    success = c.check_topology_database(date) and success
    success = c.check_merged_topology_database(date + timedelta(days=-1)) and success
    if store_results_in_db:
        success = c.check_db_results_connection() and success

    if success:
        command = "docker exec -it dfoh_rundaily python3 orchestrator.py \
            --date={}T00:00:00 \
            --db_dir=\"/tmp/db/\" \
            --store_results_in_db={} \
            --store_results_in_file={} \
            --max_workers=1".format(date.strftime("%Y-%m-%d"), store_results_in_db, store_results_in_file)
            # max_workers needs to be set to 1 because bgproutesapi does not support parallel downloads by default.
            # If you want to use parallel downloads, please contact the team: contact@bgproutes.io
        
        run_cmd(db_dir, "unistrahijackdetection/dfoh_newedge", [command], date, 'new_edge')


def run_broker_inference(date, db_dir,store_results_in_db, store_results_in_file):
    print (print_prefix()+"START: {} {}".format(date, 'inference'))

    if not os.path.isdir(db_dir+'/cases'):
        os.mkdir(db_dir+'/cases')

    # First check whether all the data is there.
    c = Checker(db_dir)

    success = True
    if store_results_in_file:
        success = c.check_newedges(date) and success
    if store_results_in_db:
        success = c.check_newedges_in_db(date) and success

    if success:
        # Parameters of the broker
        features = "aspath,bidirectionality,peeringdb,topological"
        nb_days_training=300
        fn = "{}/new_edge/{}.txt".format(db_dir, date.strftime("%Y-%m-%d")) if store_results_in_file else None

        command = "{} runner/broker.py \
            --date={} \
            --db_dir={} \
            --store_results_in_db=\"{}\" \
            --input_file={} \
            --features={} \
            --nb_days_training={} \
            --outfile={}".format(sys.executable,
                                 date.strftime("%Y-%m-%d"),
                                 db_dir,
                                 store_results_in_db,
                                 fn,
                                 features, 
                                 nb_days_training,
                                 "{}/cases/{}.tmp".format(db_dir, date.strftime("%Y-%m-%d")))

        with open('logs/{}_{}.txt'.format(date.strftime("%Y-%m-%d"), "inference"), 'w') as fd:
            subprocess.run(command, shell=True, stderr=fd)

    print (print_prefix()+"DONE: {} {}".format(date, 'inference'))

def run_accuracy(date, db_dir):
    print (print_prefix()+"START: {} {}".format(date, 'accuracy'))

    # First check whether all the data is there.
    c = Checker(db_dir)

    success = True
    success = c.check_inference_models(date) and success

    if success:
        for features in \
        ['aspath,bidirectionality,peeringdb,topological', \
        'bidirectionality,peeringdb,topological', \
        "aspath,peeringdb,topological", \
        "aspath,bidirectionality,topological", \
        "aspath,bidirectionality,peeringdb"]:
            accuracy(date.strftime("%Y-%m-%d"), db_dir, features, [1,5,10])
    
    clusters(date.strftime("%Y-%m-%d"), db_dir)

 
def run_parser(date, db_dir, store_results_in_db, store_results_in_file):
    print (print_prefix()+"START: {} {}".format(date, 'parser'))

    # First check whether all the data is there.
    c = Checker(db_dir)

    success = True
    success = c.check_cases(date) and success

    if success:
        launch_parser(db_dir=db_dir, date=date, store_results_in_db=store_results_in_db, store_results_in_file=store_results_in_file)


def run_day(db_dir, store_results_in_db, store_results_in_file, day=None):
    if day is None: # Then day is now.
        day = datetime.today() - timedelta(days=1)
        day = day.replace(minute=0, second=0, microsecond=0)

    print(f'[{datetime.now()}] Running DFOH for {day.strftime("%Y-%m-%d")}...')

    # Gather the database, needed to compute the feature values.
    run_database(day, db_dir)

    # Run the sampling, needed for the training.
    run_sampling(day, db_dir)

    # Compute all the features values.
    run_features(day, db_dir)

    # Find all the new edges (legitimate and suspicious).
    run_new_edges(day, db_dir, store_results_in_db, store_results_in_file)

    # Run DFOH on the new edge cases.
    run_broker_inference(day, db_dir, store_results_in_db, store_results_in_file)

    # Parse the results and infer the suspicious cases.
    run_parser(day, db_dir, store_results_in_db, store_results_in_file)

    # Generate the accuracy grids.
    # run_accuracy(d, db_dir)

# Make the CLI.
@click.command()
@click.option('--date', help='Start date in the format "YYYY-MM-DD".', type=str, default=None)
@click.option('--date_end', help='End date in the format "YYYY-MM-DD".', type=str, default=None)
@click.option('--live', help='If this is enable, the script execute every day, for the current day. It is a live mode but that executes once a day.', type=bool, default=False)
@click.option('--db_dir', default="/root/type1_main/setup/db2", help='Directory where is database.', type=str)
@click.option('--store_results_in_file', is_flag=True, help='If set, store the inference and new links results in a file.')
@click.option('--store_results_in_db', is_flag=True, help='If set, store the inference and new links in a PostgreSQL database (needs to be configured before running this script, see doc for more info).')

def launch_checker(\
    date,\
    date_end,\
    db_dir, \
    live, \
    store_results_in_file, \
    store_results_in_db):
    """
    This script runs DFOH for a given day.
    It downloads all the file, performs all intermediate steps
    (merger, sampling, new edge) and finally runs DFOH.
    If some input files are missing for some previous days,
    an error is written and DFOH will not execute. 
    """
    if not (store_results_in_db or store_results_in_file):
        raise click.UsageError("You must specify at least one of --store_results_in_file or --store_results_in_db.")

    if date is not None and date_end is not None:

        date = datetime.strptime(date, "%Y-%m-%d")
        edate = datetime.strptime(date_end, "%Y-%m-%d")

        # Create the log dir if it does not exist.
        if not os.path.exists('logs'):
            os.makedirs('logs')

        for d in daterange(date, edate):
            run_day(db_dir, store_results_in_db, store_results_in_file, d)

    if live:
        schedule.every().day.at("10:09").do( \
            run_day, \
            db_dir=db_dir, \
            store_results_in_db=store_results_in_db, \
            store_results_in_file=store_results_in_file)
        while True:
            schedule.run_pending()
            time.sleep(1)


if __name__ == "__main__":
    launch_checker()
