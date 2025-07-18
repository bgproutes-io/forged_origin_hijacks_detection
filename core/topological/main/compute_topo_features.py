import networkx as nx
from time import time
from colorama import Fore, Style
import sys
from datetime import datetime


res_zero = {"closeness_centrality_as1": 0, \
        "closeness_centrality_as2": 0, \
        "harmonic_centrality_as1": 0, \
        "harmonic_centrality_as2": 0, \
        "eccentricity_as1": 0, \
        "eccentricity_as2": 0, \
        "degree_centrality_as1": 0, \
        "degree_centrality_as2": 0, \
        # "number_of_cliques_as1": number_of_cliques_as1, \
        # "number_of_cliques_as2": number_of_cliques_as2, \
        "average_neighbor_degree_as1": 0, \
        "average_neighbor_degree_as2": 0, \
        "triangles_as1": 0, \
        "triangles_as2": 0, \
        "clustering_as1": 0, \
        "clustering_as2": 0,
        "shortest_path": 0,
        "jaccard": 0,
        "adamic_adar": 0,
        "preferential_attachement": 0}

def prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.YELLOW+Style.BRIGHT+"[compute_topo_features.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


warning_ = 0


def compute_pagerank(G :nx.Graph, as1, as2):
    res = nx.pagerank(G, max_iter=20)

    return res[as1], res[as2]


def compute_eigenvector_centrality(G :nx.Graph, as1, as2):
    res = nx.eigenvector_centrality(G, max_iter=20, tol=1.0e-5)

    return res[as1], res[as2]

def compute_degree_centrality(G :nx.Graph, as1, as2):
    res = nx.degree_centrality(G)

    return res[as1], res[as2]


def compute_number_of_cliques(G :nx.Graph, as1, as2):
    res_as1 = nx.number_of_cliques(G, as1)
    res_as2 = nx.number_of_cliques(G, as2)

    return res_as1, res_as2

def compute_average_neighbor_degree(G :nx.Graph, as1, as2):
    res = nx.average_neighbor_degree(G, nodes=[as1, as2])

    return res[as1], res[as2]

def compute_triangles(G :nx.Graph, as1, as2):
    res = nx.triangles(G, [as1, as2])

    return res[as1], res[as2]

def compute_clustering(G :nx.Graph, as1, as2):
    res_as1 = nx.clustering(G, as1)
    res_as2 = nx.clustering(G, as2)

    return res_as1, res_as2

def compute_square_clustering(G :nx.Graph, as1, as2):
    res_as1 = nx.square_clustering(G, as1)
    res_as2 = nx.square_clustering(G, as2)

    return res_as1, res_as2

def compute_eccentricity(G :nx.Graph, as1, as2):
    res_as1 = nx.eccentricity(G, v=as1)
    res_as2 = nx.eccentricity(G, v=as2)
    
    return res_as1, res_as2


def compute_shortest_path(G :nx.Graph, as1, as2):
    res = len(nx.shortest_path(G, source=as1, target=as2))
    return res

def compute_jaccard(G :nx.Graph, as1, as2):
    res = list(nx.jaccard_coefficient(G, [(as1, as2)]))[0][2]
    return res

def compute_adamic_adar(G :nx.Graph, as1, as2):
    res = list(nx.adamic_adar_index(G, [(as1, as2)]))[0][2]
    return res

def compute_preferential_attachment(G :nx.Graph, as1, as2):
    res = list(nx.preferential_attachment(G, [(as1, as2)]))[0][2]
    return res

def compute_simrank_similarity(G :nx.Graph, as1, as2):
    res = nx.simrank_similarity(G, source=as1, target=as2)
    return res

##############################################################
## Optimization of the harmonic and closeness centrality #####
## computation. The optimization leverages the fact that #####
## the weights are always symetric. Besides, it computes #####
## both features with a single shortest path computation ##### 
##############################################################


def compute_harmonic_closeness_centrality_eccentricity_aux(G: nx.Graph, focus):
    dists = nx.shortest_path_length(G, source=focus, weight=None)

    # Conputation of the harmonic centrality.
    harm = 0
    for d in dists.values():
        if d == 0:
            continue
        harm += 1 / d

    # computation of the closeness centrality
    totsp = sum(dists.values())
    if totsp > 0.0 and len(G) > 1:
        clos = (len(dists)-1.0) / totsp
        s = (len(dists)-1.0) / ( len(G) - 1 )
        clos *= s
    else:
        clos = 0.0

    ecce = max(dists.values())

    return harm, clos, ecce

def compute_harmonic_closeness_centrality_eccentricity(G: nx.Graph, as1, as2):
    harm_as1, clos_as1, ecce_as1 = compute_harmonic_closeness_centrality_eccentricity_aux(G, as1)
    harm_as2, clos_as2, ecce_as2 = compute_harmonic_closeness_centrality_eccentricity_aux(G, as2)

    return harm_as1, clos_as1, ecce_as1, harm_as2, clos_as2, ecce_as2


# dictionary with all the callback functions assiciated with
# a specific key that represents the feature name, for the 
# per-node graph features
base_func = {
    "pagerank": compute_pagerank,
    "eigenvector_centrality": compute_eigenvector_centrality,
    "degree_centrality": compute_degree_centrality,
    "number_of_cliques": compute_number_of_cliques,
    "average_neighbor_degree": compute_average_neighbor_degree,
    "triangles": compute_triangles,
    "clustering": compute_clustering,
    "square_clustering": compute_square_clustering
    }

# dictionary with all the callback functions assiciated with
# a specific key that represents the feature name, for the 
# per-link graph features
base_func_link = {
    "shortest_path": compute_shortest_path,
    "jaccard": compute_jaccard,
    "adamic_adar": compute_adamic_adar,
    "preferential_attachement": compute_preferential_attachment,
    "simrank_similarity": compute_simrank_similarity
}



####
# This function computes all the graph features, but exculde some of these features
# 
# @param G              Networkx undirected graph
# @param as1            First focus AS
# @param as2            Second focus AS
# @param feat_exclude   Features to exclude during the computation
####

def compute_all_features(G :nx.Graph, as1, as2, feat_exclude=["pagerank", "eigenvector_centrality", "square_clustering", "number_of_cliques", "simrank_similarity"]):

    results = dict()

    # Switch for convention
    if int(as1) > int(as2):
        as1, as2 = as2, as1

    # Add the two AS in the result dictionary
    results["as1"] = as1
    results["as2"] = as2

    # First compute the features for all the per-node features
    for feat in base_func:
        if feat not in feat_exclude:
            if warning_:
                start = time()
            try:
                results["{}_as1".format(feat)], results["{}_as2".format(feat)] = base_func[feat](G, as1, as2)
            except:
                # If a Networkx error occur, notify it
                results["{}_as1".format(feat)], results["{}_as2".format(feat)] = None, None

            if warning_:
                stop = time() - start
                prefix("compute_{} for link {}-{} took {:.4f} s".format(feat, as1, as2, stop))

    # Then compute the features for all the per-edge features
    for feat in base_func_link:
        if feat not in feat_exclude:
            if warning_:
                start = time()
            try:
                results[feat] = base_func_link[feat](G, as1, as2)
            except:
                # If a Networkx error occur, notify it
                results[feat] = None

            if warning_:
                stop = time() - start
                prefix("compute_{} for link {}-{} took {:.4f} s".format(feat, as1, as2, stop))


    # Compute the harmonic-closeness centrality. With the optimisation, it
    # cannot be treated the same way as the others per-node features
    if warning_:
        start = time()
    try:
        results["harmonic_centrality_as1"], results["closeness_centrality_as1"], results["eccentricity_as1"], results["harmonic_centrality_as2"], results["closeness_centrality_as2"], results["eccentricity_as2"] = compute_harmonic_closeness_centrality_eccentricity(G, as1, as2)
    except:
        results["harmonic_centrality_as1"], results["closeness_centrality_as1"], results["eccentricity_as1"], results["harmonic_centrality_as2"], results["closeness_centrality_as2"], results["eccentricity_as2"] = None, None, None, None, None, None

    if warning_:
        stop = time() - start
        prefix("compute_harmonic_closeness_centrality for link {}-{} took {:.4f} s".format(as1, as2, stop))

    
    return results

if __name__ == "__main__":
    G = nx.barabasi_albert_graph(10000, 10)
    as1 = list(G.nodes())[0]
    as2 = list(G.nodes())[1]
    res = compute_all_features(G, as1, as2)
    print(res)
    


