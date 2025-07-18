import os
import pandas as pd
from time import time
import shlex
import random
import string
import subprocess
import utils as ut


def copy_file_to_container(fn, docker_name, id):
    cmd = shlex.split("docker cp {} {}:/tmp/{}_{}".format(fn, docker_name, os.path.basename(fn), id))
    subprocess.run(cmd)
    return "/tmp/{}_{}".format(os.path.basename(fn), id)

def remove_file_in_container(fn, docker_name):
    cmd = shlex.split("docker exec -it {} rm {}".format(docker_name, fn))
    subprocess.run(cmd)

def start_container(db_dir, docker_image, docker_name):
    # kill and remove the docker container.
    subprocess.run("docker kill {}".format(docker_name), shell=True, stdout=subprocess.DEVNULL)
    subprocess.run("docker rm {}".format(docker_name), shell=True, stdout=subprocess.DEVNULL)

    # Run the docker container.
    docker_cmd = "docker run -itd --name=\"{}\" \
    --hostname=\"{}\" \
    --add-host=host.docker.internal:host-gateway \
    -e DFOH_DB_NAME={} \
    -e DFOH_DB_USER={} \
    -e DFOH_DB_PWD={} \
    -v {}:/tmp/db \
    {}".format(docker_name, docker_name, os.getenv('DFOH_DB_NAME'), os.getenv('DFOH_DB_USER'), os.getenv('DFOH_DB_PWD'), db_dir, docker_image)
    subprocess.run(docker_cmd, shell=True, stdout=subprocess.DEVNULL)

def stop_container(docker_name):
    # kill and remove the docker container.
    subprocess.run("docker kill {}".format(docker_name), shell=True, stdout=subprocess.DEVNULL)
    subprocess.run("docker rm {}".format(docker_name), shell=True, stdout=subprocess.DEVNULL)

def run_aspath_features(asplist, date, db_dir, store_results_in_db, fn=None, id=0, docker_name='dfoh_aspathfeat'):

    # Start the container.
    start_container(db_dir, "unistrahijackdetection/dfoh_aspathfeat", docker_name)

    args = ["docker", "exec", "-i", docker_name, "python3", "aspath_feat.py", "--date={}".format(date)]

    file_copied_to_container = False

    if store_results_in_db:
        args.append("--store_results_in_db")
    elif fn:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, docker_name, id)
        file_copied_to_container = True

        # Complete the parameter value.
        args.append("--aspath_file={}".format(fn_tmp))
    else:
        args.append("--aspath_list")
        list_aspath = ""

        for (as1, as2, asp) in asplist:
            list_aspath += "{} {},{}-".format(as1, as2, asp)
        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None
        args.append(list_aspath)

    # print (args)
    df = ut.command_to_csv(args)

    if file_copied_to_container:
        # Remove the input file from the container.
        remove_file_in_container(fn_tmp, docker_name)

    # Stop the container.
    stop_container(docker_name)

    return df



def run_topological_features(asplist, date, db_dir, store_results_in_db, fn=None, id=0, docker_name='dfoh_topological'):
    # Start the container.
    start_container(db_dir, "unistrahijackdetection/dfoh_topological", docker_name)

    args = ["docker", "exec", "-i", docker_name, "python3", "topo_feat.py", "--nb_threads=12", "--date={}".format(date)]

    file_copied_to_container = False
    
    if store_results_in_db:
        args.append("--store_results_in_db")
    elif fn:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, docker_name, id)
        file_copied_to_container = True

        # Complete the parameter value.
        args.append("--link_file={}".format(fn_tmp))
    else:
        args.append("--link_list")
        list_aspath = ""

        all_links = []
        for (as1, as2, _) in asplist:
            if (as1, as2) not in all_links:
                list_aspath += "{}-{},".format(as1, as2)
                all_links.append((as1, as2))

        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None
        
        args.append(list_aspath)

    # print (args)
    df = ut.command_to_csv(args)

    if file_copied_to_container:
        # Remove the input file from the container.
        remove_file_in_container(fn_tmp, docker_name)

    # Stop the container.
    stop_container(docker_name)

    return df


def run_bidir_features(asplist, date, db_dir, store_results_in_db, fn=None, id=0, docker_name='dfoh_bidirectionality'):
    # Start the container.
    start_container(db_dir, "unistrahijackdetection/dfoh_bidirectionality", docker_name)

    args = ["docker", "exec", "-i", docker_name, "python3", "orchestrator.py", "--db_dir=/tmp/db", "--date={}".format(date)]

    file_copied_to_container = False

    if store_results_in_db:
        args.append("--store_results_in_db")
    elif fn:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, docker_name, id)
        file_copied_to_container = True

        # Complete the parameter value.
        args.append("--link_file={}".format(fn_tmp))
    else:
        args.append("--link_list")
        list_aspath = ""

        all_links = []
        for (as1, as2, _) in asplist:
            if (as1, as2) not in all_links:
                list_aspath += "{}-{},".format(as1, as2)
                all_links.append((as1, as2))

        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None
        
        args.append(list_aspath)

    # print (args)
    df = ut.command_to_csv(args)

    if file_copied_to_container:
        # Remove the input file from the container.
        remove_file_in_container(fn_tmp, docker_name)

    # Stop the container.
    stop_container(docker_name)

    return df


def run_peeringdb_features(asplist, date, db_dir, store_results_in_db, fn=None, id=0, docker_name='dfoh_peeringdb', clean=True):
    # Start the container.
    start_container(db_dir, "unistrahijackdetection/dfoh_peeringdb", docker_name)

    args = ["docker", "exec", "-i", docker_name, "python3", "orchestrator.py", \
        "--db_dir=/tmp/db", \
        "--date={}".format(date), \
        "--clean={}".format(clean)]
    
    file_copied_to_container = False

    if store_results_in_db:
        args.append("--store_results_in_db")
    elif fn:
        # Copy the file in the docker container.
        fn_tmp = copy_file_to_container(fn, docker_name, id)
        file_copied_to_container = True

        # Complete the parameter value.
        args.append("--link_file={}".format(fn_tmp))
    else:
        args.append("--link_list")
        list_aspath = ""

        all_links = []
        for (as1, as2, _) in asplist:
            if (as1, as2) not in all_links:
                list_aspath += "{}-{},".format(as1, as2)
                all_links.append((as1, as2))

        if len(list_aspath):
            list_aspath = list_aspath.rstrip(list_aspath[-1])
        else:
            return None
        
        args.append(list_aspath)

    # print (args)
    df = ut.command_to_csv(args)

    if file_copied_to_container:
        # Remove the input file from the container.
        remove_file_in_container(fn_tmp, docker_name)

    # Stop the container.
    stop_container(docker_name)

    return df


def run_features(asplist, date, db_dir, features, fn=None, store_results_in_db=False, id=0, suffix_name='', peeringdb_clean=True):
    df = dict()

    if 'bidirectionality' in features:
        start = time()
        df["bidirectionality"] = run_bidir_features(asplist, date, db_dir, store_results_in_db, id=id, docker_name='dfoh_bidirectionality'+suffix_name)
        ut.wrn_msg("Bidirectionality feature took {:.2f} seconds".format(time() - start))
    
    if 'peeringdb' in features:
        start = time()
        df["peeringdb"] = run_peeringdb_features(asplist, date, db_dir, store_results_in_db, id=id, docker_name='dfoh_peeringdb'+suffix_name, clean=peeringdb_clean)
        ut.wrn_msg("Peeringdb feature took {:.2f} seconds".format(time() - start))
    
    if 'topological' in features:
        start = time()
        df["topological"] = run_topological_features(asplist, date, db_dir, store_results_in_db, id=id, docker_name='dfoh_topological'+suffix_name)
        ut.wrn_msg("Topological feature took {:.2f} seconds".format(time() - start))
    
    if 'aspath' in features:
        start = time()
        ut.wrn_msg("ASpath input: ".format(time() - start))

        df["aspath"] = run_aspath_features(asplist, date, db_dir, store_results_in_db, id=id, docker_name='dfoh_aspath'+suffix_name)
        ut.wrn_msg("ASpath feature took {:.2f} seconds".format(time() - start))
    
    feat_available = []

    for feat in df.keys():
        if df[feat] is not None:
            feat_available.append(feat)
            with open("tmp_values_{}.txt".format(feat), "w") as f:
                f.write(df[feat].to_csv(index=False, sep=" "))

    if len(feat_available) == 0:
        ut.err_msg("No feature type are available, abort...")
        return None, None


    feat_ref = feat_available[0]


    X = df[feat_ref]
    # print(X.keys())

    for feat in feat_available[1:]:
        X = X.merge(df[feat], how="inner", on=["as1", "as2"])
        # print(X.keys())

    return X, feat_available


def run_inference(in_df, db_dir, feats, date, nb_days_training_data, id, docker_name='dfoh_inference', fpr_weights="1,2,3,4,5,6,7,8,9,10"):
    # Start the container.
    start_container(db_dir, "unistrahijackdetection/dfoh_inference", docker_name)

    with open(db_dir+"/tmp/inference_{}.txt".format(id), "w") as f:
        f.write(in_df.to_csv(index=False, sep=" "))

    args = ["docker", "exec", \
            "-i", docker_name, \
            "python3", "inference_maker.py", \
            "--date={}".format(date), \
            "--input_file=/tmp/db/tmp/inference_{}.txt".format(id), \
            "--fpr_weights={}".format(fpr_weights), \
            "--overide=1", \
            "--nb_days_training_data={}".format(nb_days_training_data)]

    #args.append(ut.csv_to_string(in_df))

    args.append("--features={}".format(",".join(feats)))

    # print (args)

    df = ut.command_to_csv(args)

    # Stop the container.
    stop_container(docker_name)

    # Remove the files with the feature values.
    os.remove(db_dir+"/tmp/inference_{}.txt".format(id))

    if df is None:
        return None

    return df



if __name__ == "__main__":
    date = "2022-01-02"
    asplist = []
    asplist.append(("12389", "15497", "395152 14007 3356 20764 12389 15497"))
    asplist.append(("12389", "50673", "20634 8447 20764 12389 50673"))
    asplist.append(("12389", "50673", "8676 3356 174 20764 12389 50673"))
    asplist.append(("12389", "20764", "8676 3356 174 20764 12389 50673"))

    df, feats = run_features(asplist, date)
    res = run_inference(df, feats, date)

    print(res)


