import utils as ut
import matplotlib.pyplot as plt


def load_dataset(date, dir):
    fn = "{}/sampling_cluster/{}.txt".format(dir, date)
    degrees = ut.get_all_degrees(date, dir)
    cones = ut.get_all_cone_sizes(date, dir)

    g_degrees = dict()
    g_cones = dict()
    labels = list()
    with open(fn, 'r') as f:
        for line in f:
            linetab = line.strip("\n").split(" ")
            asn = linetab[0]
            lab = linetab[1]

            if asn not in degrees or asn not in cones or asn in ut.tier_one:
                continue
            
            if lab not in g_degrees:
                g_degrees[lab] = list()
                g_cones[lab] = list()

                labels.append(lab)

            
            g_degrees[lab].append(degrees[asn])
            g_cones[lab].append(cones[asn])

    colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:red', 'black', 'tab:brown', 'tab:purple', 'tab:gray', 'tab:olive', 'tab:cyan', 'darkblue', 'darkgreen', 'gold', 'lightblue', 'lightgreen', 'salmon', 'violet']

    i = 0
    for lab in sorted(list(labels), reverse=True):
        color = colors[i % len(colors)]
        i += 1
        Y = g_cones[lab]
        X = g_degrees[lab]
        plt.scatter(X, Y, c=color, marker='+', label=str(lab))

    plt.title("Clustering for sampling")
    plt.legend()
    plt.ylabel("Customer Cone Size")
    plt.xlabel("ASN degree")
    plt.tight_layout()
    plt.savefig("clusters_sampling.pdf")
    plt.clf()


