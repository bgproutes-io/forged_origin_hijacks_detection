import os
import pandas as pd
import subprocess
from io import StringIO

def start_container(db_dir, docker_image, docker_name):
    # kill and remove the docker container.
    subprocess.run("docker kill {}".format(docker_name), shell=True, text=True)
    subprocess.run("docker rm {}".format(docker_name), shell=True, text=True)

    # Run the docker container.
    docker_cmd = "docker run -itd --name=\"{}\" \
    --hostname=\"{}\" \
    -v {}:/tmp/db \
    {}".format(docker_name, docker_name, db_dir, docker_image)
    subprocess.run(docker_cmd, shell=True, text=True)

def stop_container(docker_name):
    # kill and remove the docker container.
    subprocess.run("docker kill {}".format(docker_name), shell=True, text=True)
    subprocess.run("docker rm {}".format(docker_name), shell=True, text=True)

def command_to_csv(args, debug=False):
    if debug:
        subprocess.run(args)
        return None

    res = subprocess.run(args, stdout=subprocess.PIPE)
    if res.returncode != 0:
        print("Command \"{}\" failed".format(" ".join(args)))
        return None
    
    output = res.stdout.decode()

    s = ""
    for line in output.split("\n"):
        if "[" in line:
            continue
        s += line + "\n"


    io_data = StringIO(s)

    #try:
    df = pd.read_csv(io_data, sep=" ")
    # except:
    #     err_msg("Unable to transform command {} to CSV format".format(" ".join(args)))
    #     return None
    
    return df

def load_features(date, db_dir, features, edge_class):
    df = dict()

    s = '' if edge_class == 'negative' else '_clusters'

    print (features)
    if 'bidirectionality' in features:
        df["bidirectionality"] = pd.read_csv("{}/features/{}/bidirectionality{}/{}_{}.txt".format(db_dir, edge_class, s, date, edge_class), sep=' ')
    
    if 'peeringdb' in features:
        df["peeringdb"] = pd.read_csv("{}/features/{}/peeringdb{}/{}_{}.txt".format(db_dir, edge_class, s, date, edge_class), sep=' ')

    if 'topological' in features:
        df["topological"] = pd.read_csv("{}/features/{}/topological{}/{}_{}.txt".format(db_dir, edge_class, s, date, edge_class), sep=' ')
    
    if 'aspath' in features:
        df["aspath"] = pd.read_csv("{}/features/{}/aspath{}/{}_{}.txt".format(db_dir, edge_class, s, date, edge_class), sep=' ')
    
    feat_available = []

    for feat in df.keys():
        if df[feat] is not None:
            feat_available.append(feat)
            with open("tmp_values_{}.txt".format(feat), "w") as f:
                f.write(df[feat].to_csv(index=False, sep=" "))

    feat_ref = feat_available[0]

    X = df[feat_ref]
    print(X.keys())

    for feat in feat_available[1:]:
        X = X.merge(df[feat], how="inner", on=["as1", "as2"])
        print(X.keys())

    return X, feat_available


def run_inference(in_df, db_dir, feats, date, nb_days_training_data):
    docker_name = 'type1_accuracy_inference'

    # Start the container.
    start_container(db_dir, "unistrahijackdetection/type1_inference", docker_name)

    with open(db_dir+"/tmp/accuracy_inference_{}.txt".format(date), "w") as f:
        f.write(in_df.to_csv(index=False, sep=" "))

    args = ["docker", "exec", \
            "-i", docker_name, \
            "python3", "inference_maker.py", \
            "--date={}".format(date), \
            "--input_file=/tmp/db/tmp/accuracy_inference_{}.txt".format(date), \
            "--fpr_weights=1,2,3,4,5,6,7,8,9,10", \
            "--overide=0", \
            "--nb_days_training_data={}".format(nb_days_training_data)]

    args.append("--features={}".format(",".join(feats)))

    print (args)

    res_df = command_to_csv(args)

    # Stop the container.
    stop_container(docker_name)

    # Remove the files with the feature values.
    os.remove(db_dir+"/tmp/accuracy_inference_{}.txt".format(date))

    # That should not happen.
    if res_df is None:
        print ('Unexpected empty dataframe.')
        return None

    # Read the resulting output.
    results = []
    for i in range(0, len(res_df.index)):
        line = dict()
        for feat in res_df.keys():
            line[feat] = str(res_df[feat].values[i])

        results.append(line.copy())

    # Make the result into text format.
    s = ''
    for l in results:
        s += l['as1']+' '
        s += l['as2']+' '
        if 'asp' in l:
            s += l['asp']+' '
        else:
            s += 'None'+' '
        s += l['label']+' '
        s += l['proba']+' '
        s += l['sensitivity']+'\n'
    
    return s[:-1]
