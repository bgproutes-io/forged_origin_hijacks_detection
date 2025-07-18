import pandas as pd
import matplotlib.pyplot as plt
import statistics as st
import numpy as np


def main():
    X = pd.read_csv("results.txt", sep=" ")
    groups = X.groupby(X.SIZE)

    g = sorted(groups.groups.keys())

    datas = []
    for key in g:
        x = groups.get_group(key)["FPR"].values
        datas.append(x)


    plt.boxplot(datas)
    plt.xticks(np.arange(1, 8, 1), ["200", "500", "2000", "5000", "10000", "20000", "40000"], fontsize=13)
    plt.xlabel("Dataset Size", fontsize=25, labelpad=15)
    plt.ylabel("FPR", fontsize=25, labelpad=15)
    plt.tick_params(axis='both', which='major', pad=15)
    plt.tight_layout()

    plt.savefig("FPR_per_size.pdf")
    plt.clf()


    datas = []
    for key in g:
        x = groups.get_group(key)["TPR"].values
        datas.append(x)


    plt.boxplot(datas)
    plt.xticks(np.arange(1, 8, 1), ["200", "500", "2000", "5000", "10000", "20000", "40000"], fontsize=13)
    plt.xlabel("Dataset Size", fontsize=25, labelpad=15)
    plt.ylabel("TPR", fontsize=25, labelpad=15)
    plt.tick_params(axis='both', which='major', pad=15)
    plt.tight_layout()

    plt.savefig("TPR_per_size.pdf")
    plt.clf()

main()