import os
import click
import networkx as nx
from datetime import datetime, timedelta

from accuracy.utils import load_features, run_inference
from accuracy.heatmap import plot_heatmap_rate
from accuracy.clusters import make_figure

def accuracy(date, db_dir, features, suspicious_levels):

    if date is None:
        print("Please provide a date with option --date")
        exit(1)

    if db_dir is None:
        print("Please provide a db_dir with option --date")
        exit(1)

    if not os.path.isdir(db_dir+'/accuracy'):
        os.mkdir(db_dir+'/accuracy')

    for suspicious_level in suspicious_levels:
        outfile_tn = '{}/accuracy/{}_{}_{}_tn.txt'.format(db_dir, date, ','.join(sorted(features.split(','))), suspicious_level)
        outfile_fn = '{}/accuracy/{}_{}_{}_fn.txt'.format(db_dir, date, ','.join(sorted(features.split(','))), suspicious_level)
        outfile_tp = '{}/accuracy/{}_{}_{}_tp.txt'.format(db_dir, date, ','.join(sorted(features.split(','))), suspicious_level)
        outfile_fp = '{}/accuracy/{}_{}_{}_fp.txt'.format(db_dir, date, ','.join(sorted(features.split(','))), suspicious_level)
        outfile_tn_fd = open(outfile_tn, 'w')
        outfile_fn_fd = open(outfile_fn, 'w')
        outfile_tp_fd = open(outfile_tp, 'w')
        outfile_fp_fd = open(outfile_fp, 'w')


        X_neg, feat_available_neg = load_features(date, db_dir, features, "negative")
        res_neg = run_inference(X_neg, db_dir, feat_available_neg, date, 60)
        for line in res_neg.split('\n'):
            linetab = line.split(' ')

            if len(linetab) == 6:
                as1 = linetab[0]
                as2 = linetab[1]
                asp = linetab[2]
                label = int(linetab[3])
                prob = float(linetab[4])
                sus = int(linetab[5])

                if sus == suspicious_level:
                    if label == 0:
                        outfile_tn_fd.write("{} {}\n".format(as1, as2))
                    if label == 1:
                        outfile_fp_fd.write("{} {}\n".format(as1, as2))
            else:
                print ("Error output line: {}".format(line))

        X_pos, feat_available_pos = load_features(date, db_dir, features, "positive")
        res_pos = run_inference(X_pos, db_dir, feat_available_pos, date, 60)
        for line in res_pos.split('\n'):

            if len(linetab) == 6:
                linetab = line.split(' ')
                as1 = linetab[0]
                as2 = linetab[1]
                asp = linetab[2]
                label = int(linetab[3])
                prob = float(linetab[4])
                sus = int(linetab[5])

                if sus == suspicious_level:
                    if label == 1:
                        outfile_tp_fd.write("{} {}\n".format(as1, as2))
                    if label == 0:
                        outfile_fn_fd.write("{} {}\n".format(as1, as2))
            else:
                print ("Error output line: {}".format(line))
        
        outfile_tn_fd.close()
        outfile_fn_fd.close()
        outfile_tp_fd.close()
        outfile_fp_fd.close()

        plot_heatmap_rate(date, db_dir, 
            outfile_tp, outfile_fn, 
            "{}/accuracy/{}_{}_{}_TPR.pdf".format(db_dir, date, ','.join(sorted(features.split(','))), suspicious_level), cbarbool=False, thres=True)

        plot_heatmap_rate(date, db_dir, 
            outfile_fp, outfile_tn, 
            "{}/accuracy/{}_{}_{}_FPR.pdf".format(db_dir, date, ','.join(sorted(features.split(','))), suspicious_level), cbarbool=False, thres=True)




def clusters(date, db_dir):
    # Compute degree and cone size of every AS.
    merged_topo_fn = "{}/merged_topology/{}.txt".format(db_dir, date)
    G = nx.Graph()

    with open(merged_topo_fn, 'r') as fd:
        for line in fd.readlines():
            as1 = line.strip("\n").split(" ")[0]
            as2 = line.strip("\n").split(" ")[1]
            G.add_edge(as1, as2)

    print ("Parsed topo")

    # Compute cone of every AS.
    month_first_day = datetime.strptime(date, "%Y-%m-%d").replace(day=1, hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d")
    cone_fn = "{}/cone/{}.txt".format(db_dir, month_first_day)
    conesizes = {}
    with open(cone_fn, 'r') as fd:
        for line in fd.readlines():
            as1 = line.strip("\n").split(" ")[0]
            conesize = int(line.strip("\n").split(" ")[1])
            conesizes[as1] = conesize

    print ("Parsed cone")

    cluster_fn = "{}/sampling_cluster/{}.txt".format(db_dir, date)

    with open(cluster_fn, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('#'):
                nb_clusters = int(line.split(' ')[3])+1
                cluster_info = []
                for i in range(0, nb_clusters):
                    cluster_info.append([0, [], []])

            else:
                linetab = line.rstrip('\n').split(' ')
                as1 = linetab[0]
                cluster_id = int(linetab[1])
                if as1 in conesizes and as1 in G:
                    cluster_info[cluster_id][0] += 1
                    cluster_info[cluster_id][1].append(G.degree[as1])
                    cluster_info[cluster_id][2].append(conesizes[as1])

    outfile = "{}/accuracy/{}_clusters.pdf".format(db_dir, date)
    make_figure(cluster_info, outfile)





if __name__ == "__main__":
    clusters("2022-01-01", "/root/type1_main/setup/db2/")