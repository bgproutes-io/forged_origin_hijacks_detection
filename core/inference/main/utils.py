from colorama import Fore, Style
import datetime
import os
import sys


# def compute_prediction_stats(pred, Y):
#     FN = 0
#     TN = 0
#     TP = 0
#     FP = 0

#     if len(pred) != len(Y):
#         print("Error inconsistent length of vectors", file=sys.stderr)
#         exit(1)

#     for i in range(0, len(Y)):
#         if Y[i] == 1:
#             if pred[i] == 1:
#                 TP += 1
#             else:
#                 FN += 1

#         else:
#             if pred[i] == 1:
#                 FP += 1
#             else:
#                 TN += 1

#     print("Stats:\nAccuracy : {:.2f}\nTPR : {:.2f}\nFPR : {:.2f}\nTP : {}\nFP : {}\nFN : {}\nTN : {}".format(
#         (TP + TN) / (TP + TN + FP + FN) * 100, TP / (TP + FN) * 100, FP / (TN + FP) * 100, TP, FP, FN, TN
#     ))


# def return_tpr_fpr(pred, Y):
#     FN = 0
#     TN = 0
#     TP = 0
#     FP = 0

#     if len(pred) != len(Y):
#         print("Error inconsistent length of vectors", file=sys.stderr)
#         exit(1)

#     for i in range(0, len(Y)):
#         if Y[i] == 1:
#             if pred[i] == 1:
#                 TP += 1
#             else:
#                 FN += 1

#         else:
#             if pred[i] == 1:
#                 FP += 1
#             else:
#                 TN += 1

#     return TP / (TP + FN) * 100, FP / (TN + FP) * 100


def err_msg(msg, end="\n"):
    currentTime = datetime.datetime.now().strftime("%H:%M:%S")
    s = Fore.RED+Style.BRIGHT+"[ERROR ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)

def wrn_msg(msg, end="\n"):
    currentTime = datetime.datetime.now().strftime("%H:%M:%S")
    s = Fore.YELLOW+Style.BRIGHT+"[WARNING ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


def get_the_last_n_days(d, n):
    dates = []
    tod = datetime.date.fromisoformat(d)
    for i in range(1, n+1):
        delta = datetime.timedelta(days = i)
        dates.append(str(tod - delta))

    return dates


def create_directory(dir):
    if not os.path.isdir(dir):
        os.mkdir(dir)


# def write_predictions_to_plot(pred, Y, X, pred_proba, features, dir="tmp"):
#     create_directory(dir)

#     file_fp = open("{}/fp_{}.txt".format(dir, "_".join(features)), "w")
#     file_fn = open("{}/fn_{}.txt".format(dir, "_".join(features)), "w")
#     file_tp = open("{}/tp_{}.txt".format(dir, "_".join(features)), "w")
#     file_tn = open("{}/tn_{}.txt".format(dir, "_".join(features)), "w")


#     if len(pred) != len(Y):
#         print("Error inconsistent length of vectors", file=sys.stderr)
#         exit(1)

#     for i in range(0, len(Y)):
#         as1 = X["as1"].values[i]
#         as2 = X["as2"].values[i]

#         if Y[i] == 1:
#             if pred[i] == 1:
#                 file_tp.write("{} {} {}\n".format(as1, as2, pred_proba[i][1]))
#             else:
#                 file_fn.write("{} {} {}\n".format(as1, as2, pred_proba[i][0]))

#         else:
#             if pred[i] == 1:
#                 file_fp.write("{} {} {}\n".format(as1, as2, pred_proba[i][1]))
#             else:
#                 file_tn.write("{} {} {}\n".format(as1, as2, pred_proba[i][0]))

#     file_fp.close()
#     file_fn.close()
#     file_tp.close()
#     file_tn.close()
