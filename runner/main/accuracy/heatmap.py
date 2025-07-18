import networkx as nx
import matplotlib.pyplot as plt
import numpy as np
import matplotlib
import os
import argparse
import copy

## Load the full graph

tier_one = ["174", "209", "286", "701", "1239", "1299", "2828", "2914", "3257", "3320", "3356", "3491", "5511", "6453", "6461", "6762", "6830", "7018", "12956", "6939", "1273", "9002", "4637", "7473"]


def load_labels(date, db_dir):
    labels = dict()
    fn = "{}/sampling_cluster/{}.txt".format(db_dir, date)

    max_lab = 0
    print (fn)
    with open(fn, "r") as f:
        for line in f:
            if '#' not in line:
                asn = line.strip("\n").split(" ")[0]
                lab = int(line.strip("\n").split(" ")[1])

                labels[asn] = lab
                max_lab = max(max_lab, lab)

    return labels, max_lab+1

# This function fills a n*n table, where each cell is the number of links
# that belong to the corresponding category, according to the thresholds.
def compute_table(date, db_dir, edges_file=None, topo=None):

    # Initialisation of the table.
    labels, max_lab = load_labels(date, db_dir)
    table = [[0 for i in range(0, max_lab)] for i in range(0, max_lab)]

    #print (table)

    if edges_file is None:
        edges_list = topo.edges()
    else:
        edges_list = []
        with open(edges_file, 'r') as fd:
            for line in fd.readlines():
                linetab = line.rstrip('\n').split(',')[0].split(" ")
                #linetab = line.rstrip('\n').split(" ")
                as1 = str(linetab[0])
                as2 = str(linetab[1])
                edges_list.append((as1, as2))

    for as1, as2 in edges_list:
        index1 = labels[as1]
        index2 = labels[as2]

            # Increment the corresponding value in the table.
        table[index1][index2] += 1
        if index1 != index2:
            table[index2][index1] += 1    

    # Transform row values into percentage.
    for t in table:
        for i in range(0, len(t)):
            if edges_file is None:
                t[i] = float("%.5f" %  (float(t[i])/float(topo.number_of_edges())))
            else:
                t[i] = float("%.5f" %  (float(t[i])/len(edges_list)))
                #t[i] = float("%.5f" %  (float(t[i])/len(edges_list)))

            #print (t)
        t[i] = np.array(t[i])

    table = np.array(table)

    return table



def compute_table_rate(date, db_dir, edges_file1, edges_file2):

    # Initialisation of the table.
    labels, max_lab = load_labels(date, db_dir)
    print ("max labels: {}".format(max_lab))
    table = [[[0,0] for i in range(0, max_lab)] for i in range(0, max_lab)]
    table_nb = [[[0,0] for i in range(0, max_lab)] for i in range(0, max_lab)]

    #print (table)

    # Parse edges from edges_file1.
    edges_list1 = []
    with open(edges_file1, 'r') as fd:
        for line in fd.readlines():
            linetab = line.rstrip('\n').split(' ')
            as1 = str(linetab[0])
            as2 = str(linetab[1])
            edges_list1.append((as1, as2))

    # Parse edges from edges_file2.
    edges_list2 = []
    with open(edges_file2, 'r') as fd:
        for line in fd.readlines():
            linetab = line.rstrip('\n').split(' ')
            as1 = str(linetab[0])
            as2 = str(linetab[1])
            edges_list2.append((as1, as2))

    print ("Length list1: {}".format(len(edges_list1)))
    print ("Length list2: {}".format(len(edges_list2)))


    for as1, as2 in edges_list1:
        if as1 not in labels or as2 not in labels:
            continue
        index1 = labels[as1]
        index2 = labels[as2]

            # Increment the corresponding value in the table.
        table[index1][index2][0] += 1
        if index1 != index2:
            table[index2][index1][0] += 1    


    for as1, as2 in edges_list2:
        if as1 not in labels or as2 not in labels:
            continue
        index1 = labels[as1]
        index2 = labels[as2]

            # Increment the corresponding value in the table.
        table[index1][index2][1] += 1
        if index1 != index2:
            table[index2][index1][1] += 1  

    # table_nb = copy.copy(table)

    # Transform row values into percentage.
    for t in table:
        for i in range(0, len(t)):
            table_nb[table.index(t)][i] = str(t[i][0])+"/"+str(t[i][0]+t[i][1])

            # Computing the TPR or TPR for that particular cell.
            if float(t[i][0])+float(t[i][1]) == 0:
                t[i] = 0
            else:
                t[i] = float("%.4f" %  (float(t[i][0])/(float(t[i][0])+float(t[i][1]))))


    # for t in table_nb:
    #     for i in range(0, len(t)):
    #         # Computing the TPR or TPR for that particular cell.
    #         t[i] = str(t[i][0])+"/"+str(t[i][0]+t[i][1])


        # t[i] = np.array(t[i])
        #print (t)

    # print (np.array(table))
    print (np.array(table_nb))

    table = np.array(table)
    table_nb = np.array(table_nb)

    return table, table_nb



def load_graph(file):
    # Create the pre graph
    g = nx.Graph()
    
    with open(file, 'r') as fd:
        for line in fd.readlines():
            if line.startswith('#'):
                continue

            linetab = line.rstrip('\n').split(' ')
            as1 = int(linetab[0])
            as2 = int(linetab[1])

            g.add_edge(as1, as2)
    return g



def plot_heatmap(date, db_dir, edges_file, outfile, title=None, thres=None):
    graph_file = "{}/merged_topology/{}.txt".format(db_dir, date)
    g = load_graph(graph_file)
    # Full graph

    thresholds = [0, 10, 50, 100, 500, 1000, 1500, 3000, 5000, 100000]
    table = compute_table(date, db_dir, edges_file=edges_file, topo=g)

    labels = []
    for i in range(0, len(table)):
        labels.append('{}'.format(i))

    fig, ax = plt.subplots()
    im, cbar = heatmap(table, labels, labels, ax=ax,
                    cmap="YlGn", cbarlabel="\% of the edges")

    if title:
        plt.title(title)

    texts = annotate_heatmap(im, threshold=thres)

    fig.tight_layout()
    plt.savefig(outfile, format='pdf')


def plot_heatmap_rate(date, db_dir, edges_file1, edges_file2, outfile, cbarbool=True, thres=False):
    # Full graph

    thresholds = [0, 10, 50, 100, 500, 1000, 1500, 3000, 5000, 100000]
    table, table_nb = compute_table_rate(date, db_dir, edges_file1, edges_file2)

    labels = []
    for i in range(0, len(table)):
        labels.append('{}'.format(i))

    fig, ax = plt.subplots()
    # print_colorbar(table, ax, cmap="YlGn", cbarlabel="Rate", vmin=0, vmax=1)

    print (labels)
    fig, ax = plt.subplots()
    im, cbar = heatmap(table, labels, labels, ax,
                    cmap="YlGn", cbarlabel="Rate", vmin=0, vmax=1)

    texts = annotate_heatmap(im, table_nb=table_nb, threshold=thres)

    ax.figure.axes[-1].yaxis.label.set_size(15)

    if not cbarbool:
        print ('No bar')
        fig.axes[1].set_visible(False)

    fig.tight_layout()
    plt.savefig(outfile, format='pdf')


def print_colorbar(data, ax :plt.Axes,
            cbar_kw={}, cbarlabel="", **kwargs):
    
    im = ax.imshow(data, **kwargs)

    fig, axe = plt.subplots()
    plt.colorbar(im, ax=axe, **cbar_kw, location="top", orientation="horizontal", pad=0.1, shrink=1, fraction=0.15, aspect=50)
    axe.remove()
    plt.savefig("pdfs/colorbar.pdf")

    plt.clf()


def heatmap(data, row_labels, col_labels, ax :plt.Axes,
            cbar_kw={}, cbarlabel="", **kwargs):
    """
    Create a heatmap from a numpy array and two lists of labels.

    Parameters
    ----------
    data
        A 2D numpy array of shape (M, N).
    row_labels
        A list or array of length M with the labels for the rows.
    col_labels
        A list or array of length N with the labels for the columns.
    ax
        A `matplotlib.axes.Axes` instance to which the heatmap is plotted.  If
        not provided, use current axes or create a new one.  Optional.
    cbar_kw
        A dictionary with arguments to `matplotlib.Figure.colorbar`.  Optional.
    cbarlabel
        The label for the colorbar.  Optional.
    **kwargs
        All other arguments are forwarded to `imshow`.
    """

    

    if not ax:
        ax = plt.gca()

    # Plot the heatmap
    im = ax.imshow(data, **kwargs)

    # Create colorbar
    cbar = ax.figure.colorbar(im, ax=ax, **cbar_kw)
    #cbar.ax.set_ylabel(cbarlabel, va="bottom")
    cbar.ax.set_ylabel(cbarlabel, rotation=-90, va="bottom")

    # Show all ticks and label them with the respective list entries.
    ax.set_xticks(np.arange(data.shape[1]), labels=col_labels)
    ax.set_yticks(np.arange(data.shape[0]), labels=row_labels)

    # Let the horizontal axes labeling appear on top.
    ax.tick_params(top=True, bottom=False,
                   labeltop=True, labelbottom=False)

    # Rotate the tick labels and set their alignment.
    plt.setp(ax.get_xticklabels(), rotation=0, ha="right",
             rotation_mode="anchor")

    # Turn spines off and create white grid.
    ax.spines[:].set_visible(False)

    ax.set_xticks(np.arange(data.shape[1]+1)-.5, minor=True)
    ax.set_yticks(np.arange(data.shape[0]+1)-.5, minor=True)
    ax.grid(which="minor", color="w", linestyle='-', linewidth=3)
    ax.tick_params(which="minor", bottom=False, left=False)


    return im, cbar


def annotate_heatmap(im, data=None, table_nb=None, valfmt="{x:.2f}",
                     textcolors=("black", "white"),
                     threshold=None, **textkw):
    """
    A function to annotate a heatmap.

    Parameters
    ----------
    im
        The AxesImage to be labeled.
    data
        Data used to annotate.  If None, the image's data is used.  Optional.
    valfmt
        The format of the annotations inside the heatmap.  This should either
        use the string format method, e.g. "$ {x:.2f}", or be a
        `matplotlib.ticker.Formatter`.  Optional.
    textcolors
        A pair of colors.  The first is used for values below a threshold,
        the second for those above.  Optional.
    threshold
        Value in data units according to which the colors from textcolors are
        applied.  If None (the default) uses the middle of the colormap as
        separation.  Optional.
    **kwargs
        All other arguments are forwarded to each call to `text` used to create
        the text labels.
    """

    if not isinstance(data, (list, np.ndarray)):
        data = im.get_array()

    # Normalize the threshold to the images color range.
    # if threshold == True:
    #     threshold = im.norm(data.max()) / 2.
    # else:
    #     threshold = im.norm(data.max())

    # Set default alignment to center, but allow it to be
    # overwritten by textkw.
    kw = dict(horizontalalignment="center",
              verticalalignment="center")
    kw.update(textkw)

    # Get the formatter in case a string is supplied
    if isinstance(valfmt, str):
        valfmt = matplotlib.ticker.StrMethodFormatter(valfmt)

    # Loop over the data and create a `Text` for each "pixel".
    # Change the text's color depending on the data.
    texts = []
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            # kw.update(color=textcolors[int(im.norm(data[i, j]) > threshold)])
            # print (data[i, j])
            t = '.'+str(valfmt(data[i, j], None)).split(".")[1] if data[i, j] < 1 else "1"
            text = im.axes.text(j, i-0.2, t, **kw, fontsize=10)
            text = im.axes.text(j, i+0.1, '\n{}'.format(table_nb[i][j]), **kw, fontsize=7)

            texts.append(text)

    return texts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', nargs='?', type=str, help='Date at which to sample the data.', required=True)
    parser.add_argument('--tp_file', nargs='?', type=str, help='File with the TPs.')
    parser.add_argument('--tn_file', nargs='?', type=str, help='File with the TNs.')
    parser.add_argument('--fp_file', nargs='?', type=str, help='File with the FPs.')
    parser.add_argument('--fn_file', nargs='?', type=str, help='File with the FNs.')

    args = parser.parse_args()
    date = args.date
    tp_file = args.tp_file
    tn_file = args.tn_file
    fp_file = args.fp_file
    fn_file = args.fn_file

# plot_heatmap("internet_topo.txt", "internet_topo.txt", "topo_real.pdf")
#plot_heatmap("../../new_sampling/graph_complete.txt", "../../new_sampling/negative_links_aspaths.txt", "topo_real.pdf")
#plot_heatmap("../../new_sampling/graph_complete.txt", "../../new_sampling/positives_links_aspaths.txt", "topo_simu.pdf")
    # date = "eval"
    db_dir = "/root/type1_main/setup/db"
    dir_file = "data_balanced"
    #features = [sorted("aspath,bidirectionality,peeringdb,topological".split(",")), ["peeringdb"], ["aspath","bidirectionality"], ["topological"], ["aspath","bidirectionality", "peeringdb"], ["aspath","bidirectionality", "topological"], ["topological", "peeringdb"]]
    # feats = ["peeringdb"]
    # feats = ["bidirectionality"]
    # feats = ["topological"]
    # feats = ["aspath", "bidirectionality"]

    plot_heatmap_rate(date, db_dir, 
        tp_file, fn_file, 
        "pdfs/TPR_{}.pdf".format(date), cbarbool=False, thres=True)

    plt.clf()


    plot_heatmap_rate(date, db_dir, 
        fp_file, tn_file, 
        "pdfs/FPR_{}.pdf".format(date), cbarbool=False, thres=True)

    plt.clf()


    # features = [sorted("aspath,bidirectionality,peeringdb,topological".split(","))]
    # for feats in features:
    #     plot_heatmap_rate(date, db_dir, 
    #                         "{}/tp_{}.txt".format(dir_file, "_".join(sorted(feats))), 
    #                         "{}/fn_{}.txt".format(dir_file, "_".join(sorted(feats))), 
    #                         "pdfs/TPR_{}.pdf".format("_".join(sorted(feats))), cbarbool=False, thres=True)
        
    #     plt.clf()

    #     plot_heatmap_rate(date, db_dir, 
    #                         "{}/fp_{}.txt".format(dir_file, "_".join(sorted(feats))), 
    #                         "{}/tn_{}.txt".format(dir_file, "_".join(sorted(feats))), 
    #                         "pdfs/FPR_{}.pdf".format("_".join(sorted(feats))), cbarbool=False)

    #     plt.clf()



    # os.system("pdfcrop pdfs/FPR_aspath_bidirectionality_peeringdb_topological.pdf ../../sp/figures/FPR_all_features.pdf")
    # os.system("pdfcrop pdfs/TPR_aspath_bidirectionality_peeringdb_topological.pdf ../../sp/figures/TPR_all_features.pdf")
    # os.system("pdfcrop pdfs/colorbar.pdf ../../sp/figures/colorbar.pdf")

# 0 : Stub
# 1 : Large Transit/CDN/IXP
# 2 : Transit-3
# 3 : Transit-2/IXP
# 4 : Highly Connected
# 5 : Large CC Transit
# 6 : Transit-4
# 7 : Tier-one

#plot_heatmap(date, db_dir, "all_rostelecom_edges.txt", "pdfs/Rostelecom_edges.pdf")
#plot_heatmap_rate("results/datasets/internet_topo.txt", "pdfs/true_positives.pdf")