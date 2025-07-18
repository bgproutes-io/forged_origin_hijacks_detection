import matplotlib as mpl
mpl.use('PDF')
import matplotlib.pyplot as plt
import matplotlib.ticker as plticker
from matplotlib.ticker import ScalarFormatter
import numpy as np
import scipy as sp
import scipy.interpolate

mpl.rcParams['pdf.use14corefonts']=True

mpl.rcParams['xtick.labelsize'] = 20
mpl.rcParams['ytick.labelsize'] = 20
mpl.rcParams['xtick.major.pad'] = 10
mpl.rcParams['ytick.major.pad'] = 10
mpl.rcParams['xtick.minor.pad'] = 10
mpl.rcParams['ytick.minor.pad'] = 10

def make_figure(data, outfile):

    fig, (ax1, ax2, ax3) = plt.subplots(3, sharex=True)
    # dim = [0.14, 0.16, 0.82, 0.70]
    # axe = plt.axes(dim)

    shift = 0.08
    box = ax1.get_position()
    box.x0 = box.x0 + shift
    box.x1 = box.x1 + shift
    box.y0 = box.y0 + shift
    box.y1 = box.y1 + shift
    ax1.set_position(box)

    box = ax2.get_position()
    box.x0 = box.x0 + shift
    box.x1 = box.x1 + shift
    box.y0 = box.y0 + shift
    box.y1 = box.y1 + shift
    ax2.set_position(box)

    box = ax3.get_position()
    box.x0 = box.x0 + shift
    box.x1 = box.x1 + shift
    box.y0 = box.y0 + shift
    box.y1 = box.y1 + shift
    ax3.set_position(box)

    x = list(range(0, len(data)))
    y_nb = list(map(lambda x:x[0], data))
    y_degree = list(map(lambda x:x[1], data))
    y_cone = list(map(lambda x:x[2], data))

    ax1.bar(x, y_nb)
    ax2.boxplot(y_degree, positions=x)
    ax3.boxplot(y_cone, positions=x)

    ax1.set_ylabel("# of ASes", fontsize=15)
    ax2.set_ylabel("Degree", fontsize=15)
    ax3.set_ylabel("Cone size", fontsize=15)
    ax3.set_xlabel("Clusters ID", fontsize=15)

    # ax1.yticks(fontsize=15)
    ax1.yaxis.set_tick_params(labelsize=15)
    ax2.yaxis.set_tick_params(labelsize=15)
    ax3.yaxis.set_tick_params(labelsize=15)
    ax3.xaxis.set_tick_params(labelsize=15)

    plt.savefig(outfile, format='pdf')