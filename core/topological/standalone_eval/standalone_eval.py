import ml
import pandas as pd
import os
import click
import utils as ut

all_feats = [
    "as1",
    "as2",
    "pagerank_as1",
    "pagerank_as2",
    "eigenvector_centrality_as1",
    "eigenvector_centrality_as2",
    "degree_centrality_as1",
    "degree_centrality_as2",
    "number_of_cliques_as1",
    "number_of_cliques_as2",
    "average_neighbor_degree_as1",
    "average_neighbor_degree_as2",
    "triangles_as1",
    "triangles_as2",
    "clustering_as1",
    "clustering_as2",
    "square_clustering_as1",
    "square_clustering_as2",
    "eccentricity_as1",
    "eccentricity_as2",
    "harmonic_centrality_as1",
    "harmonic_centrality_as2",
    "closeness_centrality_as1",
    "closeness_centrality_as2",
    "shortest_path",
    "jaccard",
    "adamic_adar",
    "preferential_attachement",
    "simrank_similarity"
    ]


class StandaloneEvalTopoFeat:
    def __init__(self, date, db_dir, feature_exclude):
        self.G = None
        self.X_train = None
        self.X_test = None
        self.Y_train = None
        self.Y_test = None
        self.date = date
        self.db_dir = db_dir
        self.feature_exclude = feature_exclude

        self.df = None


    def load_dataset(self, infile):
        if infile is not None:
            if os.path.exists(infile):
                self.df = pd.read_csv(infile, sep=" ")

            else:
                print(ut.err_msg("File {} does not exist".format(infile)))
                exit(1)

        else:
            dates = ut.get_the_last_n_days(self.date, 0)

            self.df = ut.get_dataset_for_one_day(self.db_dir, self.date)


            for date in dates:
                tmp_df = ut.get_dataset_for_one_day(self.db_dir, date)

                self.df = pd.concat([self.df, tmp_df])


        for feat in all_feats:
            if not ut.not_in_feat_to_remove(feat, self.feature_exclude) and feat in self.df.keys():
                self.df = self.df.drop(columns=[feat])


        self.df = self.df.drop(columns=["as1", "as2"])
        self.X_train, self.X_test, self.Y_train, self.Y_test = ml.build_dataset(self.df)

    
    def make_inference(self):
        model = ml.forest_build(self.X_train, self.Y_train)

        pred = model.predict(self.X_test)

        ut.compute_prediction_stats(pred, self.Y_test.values.ravel())

        ml.forest_feature_importance(model, self.X_train.columns, '../tmp/feature_importance_impurity.pdf')
        ml.forest_feature_importance_feature_permutation(model, self.X_train.columns, self.X_test, self.Y_test, '../tmp/feature_importance.pdf')


@click.command()
@click.option("--date", default=None, help="Date of the link appearance, used to load the right topology", type=str)
@click.option("--infile", default=None, help="Input dataset file. Must be a CSV file where the labels are at the first line", type=str)
@click.option("--db_dir", default="/tmp/db", help="Database Directory", type=str)
@click.option("--feat_exclude", default="pagerank,eigenvector_centrality,square_clustering,number_of_cliques,simrank_similarity", help="Features to exclude during the computation", type=str)

def run_standalone_eval(date, infile, db_dir, feat_exclude):
    if date is None:
        print(ut.err_msg("Date is required, look at option --date"))
        exit(1)
    
    setf = StandaloneEvalTopoFeat(date, db_dir, feat_exclude.split(","))

    setf.load_dataset(infile)
    setf.make_inference()

if __name__ == "__main__":
    run_standalone_eval()


    
