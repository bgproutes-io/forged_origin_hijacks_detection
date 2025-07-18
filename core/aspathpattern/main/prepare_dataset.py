import pandas as pd
from utils import *


def asp_list_to_dataset(asp_list, label=None, metrics=["cone", "degree"], size=6):
    aspaths = dict()
    X = dict()

    if len(asp_list)< 1:
        err_msg("Link list provided is empty")
        return None

    # Initializing the data structures

    for j in range(1, size+1):
        for metric in metrics:
            aspaths["{}_{}".format(metric, j)] = []

            X["{}_{}".format(metric, j)] = []

    # Append the labels only if required
    if label is not None:
        aspaths["label"] = []

    # We also keep the link of the considered AS-path,
    # to merge with other features.
    aspaths["as1"] = []
    aspaths["as2"] = []
    aspaths["asp"] = []

    for (as1, as2, asp) in asp_list:
        if int(as1) > int(as2):
            as1, as2 = as2, as1

        path = aspath_to_list(asp)

        # Compute the features for all the required metrics
        links = dict()
        for metric in metrics:
            links[metric] = aspath_to_rel_list(path, metric=metric)

        # We treat the AS-path only if we were able to build the features for all the
        # metrics, and if the as-path is not empty.
        if None not in links.values() and len({len(i) for i in links.values()}) == 1:
                
            # While the size of the aspath does not corresponds to paramter "size"
            while len(links[metrics[0]]) != size:

                # If the AS-path is too long, we remove the first element
                if len(links[metrics[0]]) > size:
                    for metric in metrics:
                        links[metric].pop(0)

                # If the AS-path is too short, we add 0 at the top of the path
                else:
                    for metric in metrics:
                        links[metric].insert(0, 0)


            # We add the features values for each metric into the dataset
            for j in range(1, size+1):
                for metric in metrics:
                    aspaths["{}_{}".format(metric, j)].append(links[metric][j-1])
            
            # Append the label only if required
            if label is not None:
                aspaths["label"].append(label)

            
            aspaths["as1"].append(as1)
            aspaths["as2"].append(as2)
            aspaths["asp"].append(asp.replace(" ", "|"))


    # Here we do some data structure transformations
    for j in range(1, size+1):
        for metric in metrics:
            X["{}_{}".format(metric, j)] = aspaths["{}_{}".format(metric, j)]

    if label is not None:
        X["label"] = aspaths["label"]

    X["as1"] = aspaths["as1"]
    X["as2"] = aspaths["as2"]
    X["asp"] = aspaths["asp"]


    X_pd = pd.DataFrame(X)


    return X_pd


####
# Function that helps building the dataset for one specific
# day by merging both the positive and negative samples for
# this given day, and appending the labels.
#
# @param db_dir         database directory
# @param date           date to build for
# @param metric         considered metric
####

def get_dataset_for_one_day(db_dir, date, method, metrics=["degree", "cone"]):

    # First load the positive samples
    data_file = "{}/sampling/positive/sampling_aspath_{}/{}_positive.txt".format(db_dir, method, date)
    if os.path.exists(data_file):
        asp_list = file_to_aspaths_list(data_file)
        df_pos = asp_list_to_dataset(asp_list, label=1, metrics=metrics)

        if df_pos is None:
            wrn_msg("Positive sampling size for day {} is zero".format(date))

    else:
        return None


    # then the negative samples
    data_file = "{}/sampling/negative/sampling_aspath/{}_negative.txt".format(db_dir, date)
    if os.path.exists(data_file):
        asp_list = file_to_aspaths_list(data_file)
        df_neg = asp_list_to_dataset(asp_list, label=0, metrics=metrics)

        if df_neg is None:
            wrn_msg("Negative sampling size for day {} is zero".format(date))

    else:
        return None

    return pd.concat([df_pos, df_neg])


