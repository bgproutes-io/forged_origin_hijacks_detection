import datetime
import pandas as pd
from colorama import Fore, Style
import os

def err_msg(msg):


    return Fore.RED + Style.BRIGHT+"[ERROR]: " + Style.NORMAL + msg
def not_in_feat_to_remove(feat, feat_to_remove):

    for ftr in feat_to_remove:
        if feat in [ftr, ftr+"_as1", ftr+"_as2"]:
            return False

    return True




def get_the_last_n_days(d, n):
    dates = []
    tod = datetime.date.fromisoformat(d)
    for i in range(1, n+1):
        delta = datetime.timedelta(days = i)
        dates.append(str(tod - delta))

    return dates


def get_dataset_for_one_day(db_dir, date):
    data_file = "{}/features/topological/{}_positive.txt".format(db_dir, date)
    if os.path.exists(data_file):
        df_pos = pd.read_csv(data_file, delimiter=' ', index_col=False)
        df_pos["label"] = [1 for _ in range(0, len(df_pos.index))]

    else:
        print(err_msg("Unable to find topological features for day {}".format(date)))
        exit(1)


    data_file = "{}/features/topological/{}_negative.txt".format(db_dir, date)
    if os.path.exists(data_file):
        df_neg = pd.read_csv(data_file, delimiter=' ', index_col=False)
        df_neg["label"] = [0 for _ in range(0, len(df_neg.index))]

    else:
        print(err_msg("Unable to find topological features for day {}".format(date)))
        exit(1)

    return pd.concat([df_pos, df_neg])


####
# Function used only when running the aspath pattern features alone.
# Compute the accuracy, the TPR and the FPR of the model.
#
# @param : pred             result of model.predict
# @param : Y                real label of testing set
#
####

def compute_prediction_stats(pred, Y):
    FN = 0
    TN = 0
    TP = 0
    FP = 0

    if len(pred) != len(Y):
        print("Error inconsistent length of vectors")
        exit(1)

    for i in range(0, len(Y)):
        if Y[i] == 1:
            if pred[i] == 1:
                TP += 1
            else:
                FN += 1

        else:
            if pred[i] == 1:
                FP += 1
            else:
                TN += 1

    print("Stats:\nAccuracy : {:.2f}\nTPR : {:.2f}\nFPR : {:.2f}\nTP : {}\nFP : {}\nFN : {}\nTN : {}".format(
        (TP + TN) / (TP + TN + FP + FN) * 100, TP / (TP + FN) * 100, FP / (TN + FP) * 100, TP, FP, FN, TN
    ))  
