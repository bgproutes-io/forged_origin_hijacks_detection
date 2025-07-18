import ml
import os
import time
from colorama import Fore, Style
import utils as ut
import pandas as pd
import click
import sys
from datetime import datetime


all_feats_cat = {
    "topological" : ["as1", "as2", "degree_centrality_as1", "degree_centrality_as2", "average_neighbor_degree_as1", "average_neighbor_degree_as2", "triangles_as1", "triangles_as2", "clustering_as1", "clustering_as2", "eccentricity_as1", "eccentricity_as2", "harmonic_centrality_as1", "harmonic_centrality_as2", "closeness_centrality_as1", "closeness_centrality_as2", "shortest_path", "jaccard", "adamic_adar", "preferential_attachement"],
    "bidirectionality": ["as1", "as2", "bidi", "nb_vps"],
    "aspath" : ["as1", "as2", "degree", "cone", "cone&degree"],
    "peeringdb": ["as1", "as2", "country_dist", "facility_fac_dist", "facility_country_dist", "facility_cities_dist", "ixp_dist"]
}


def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN+Style.BRIGHT+"[inference_maker.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)



class InferenceMaker:
    def __init__(self, date, db_dir, feats, overide, fpr_weights, method, nb_days_training_data):
        self.date = date
        self.db_dir = db_dir
        self.features = feats
        self.overide = overide
        self.method = method
        self.fpr_weights = fpr_weights 
        self.results = []
        self.nb_days_training_data = nb_days_training_data

        if not os.path.isdir(self.db_dir+'/cases'):
            os.mkdir(self.db_dir+'/cases')
        if not os.path.isdir(self.db_dir+'/models'):
            os.mkdir(self.db_dir+'/models')

    def load_model(self):
        # This is set of models, one for each fpr weight.
        self.clfs = {}
        # The feature names used in the model, identical for every model (only the fpr differs).
        self.feature_names_in_ = None

        for fpr_weight in self.fpr_weights:
            fn_model = "{}/models/{}_model_{}_{}.pkl" \
                .format(self.db_dir, self.date, ",".join(sorted(self.features)), fpr_weight)

            # Load the model if existing
            if os.path.exists(fn_model) and not self.overide:

                start = time.time()
                self.clfs[fpr_weight] = ml.load_model(fn_model)
                self.feature_names_in_ = self.clfs[fpr_weight].feature_names_in_
                duration = time.time() - start
                print_prefix("Model (fpr weight={}) loaded in {:.2f} s".format(fpr_weight, duration))

            # Build it otherwise
            else:
                start = time.time()
                self.clfs[fpr_weight] = ml.build_model_for_day( \
                    self.db_dir, \
                    self.date, \
                    self.features, \
                    fpr_weight, \
                    self.method, \
                    nb_days=self.nb_days_training_data)
                self.feature_names_in_ = self.clfs[fpr_weight].feature_names_in_


                if self.clfs[fpr_weight] is None:
                    ut.err_msg("Unable to build model for day {}".format(self.date))
                    exit(1)


                duration = time.time() - start
                print_prefix("Model (fpr weight={}) built in {:.2f} s".format(fpr_weight, duration))


    def make_inference(self, df):

        # Remove non-float features for the training
        columns_to_drop = ["as1", "as2"]
        if "asp" in df.keys():
            columns_to_drop.append("asp")

        df_tmp = df.drop(columns=columns_to_drop)

        # Raise an error if some features are missing
        if sorted(list(df_tmp.keys())) != sorted(list(self.feature_names_in_)):
            ut.err_msg("Unable to make the inference, features in input and features in model does not match\n" \
                        " - in model : {}\n" \
                        " - provided : {}".format(sorted(list(self.feature_names_in_)), sorted(list(df_tmp.keys()))))
            exit(1)

        
        for fpr_weight in self.fpr_weights:

            # Sort the features to match the training model
            df_tmp = df_tmp[list(self.feature_names_in_)]

            # Make the predictions
            pred = self.clfs[fpr_weight].predict(df_tmp)
            pred_proba = self.clfs[fpr_weight].predict_proba(df_tmp)

            # Build the results with all the required
            # informations
            for i in range(0, len(pred)):
                line = dict()
                line["as1"] = df["as1"].values[i]
                line["as2"] = df["as2"].values[i]
                if "asp" in df.keys():
                    line["asp"] = df["asp"].values[i]
                line["label"] = pred[i]
                line["proba"] = pred_proba[i][pred[i]]
                line["sensitivity"] = fpr_weight

                self.results.append(line.copy())

    # transform the results of an inference into a CSV-form string
    def to_string(self):
        s = ""
        if len(self.results) < 1:
            return s

        keys = list(self.results[0].keys())


        s += " ".join(keys)
        s += "\n"

        for line in self.results:
            str_line = [str(v) for v in list(line.values())]

            s+= " ".join(str_line)
            s += "\n"

        return s

    # load the feature values from a file
    def load_from_file(self, fn):
        if os.path.exists(fn):
            try:
                X = pd.read_csv(fn, sep=" ")

                feat_to_remove = []
                for feat in X.keys():
                    if feat not in self.feature_names_in_ and feat not in ["as1", "as2", "asp"]:
                        feat_to_remove.append(feat)

                if len(feat_to_remove):
                    X = X.drop(columns=feat_to_remove)

                return X
            except:
                ut.err_msg("Unable to read file {}, please verify format".format(fn))
                exit(1)

        else:
            ut.err_msg("Unable to find file {} locally, aborting...".format(fn))
            exit(1)

    #load the feature values from a string
    def load_from_string(self, all_feats_str: str):
        all_feats = all_feats_str.split(",")

        results = dict()

        # For all the aspath we want to test,...
        for feats in all_feats:
            line = dict()

            # For all the features we get...
            for feat in feats.split("|"):

                # We extract the feature name and the value
                f = feat.split("=")[0]
                val = feat.split("=")[1]

                # If the value cannot be transformed into a
                # float, transform it into a string (unused during
                # training)
                if f in ["as1", "as2", "asp"]:
                    line[f] = str(val)
                else:
                    line[f] = float(val)
            
            # If all the input do not have the same features, raise an error
            if len(results.keys()) != 0 and sorted(list(results.keys())) != sorted(list(line.keys())):
                ut.wrn_msg("inconsistency between provided features, skipped...")
                continue

            for (feat, val) in line.items():
                if feat not in results:
                    results[feat] = []

                results[feat].append(val)

        # Transform the input into a dataframe
        X = pd.DataFrame(results)

        feat_to_remove = []
        for feat in X.keys():
            if feat not in self.feature_names_in_ and feat not in ["as1", "as2", "asp"]:
                feat_to_remove.append(feat)

        # Drop the features taht do not appears in the training set
        if len(feat_to_remove):
            X = X.drop(columns=feat_to_remove)

        return X


@click.command()
@click.option("--date", help="Date of the link appearance, used to load the right topology", type=str)
@click.option("--db_dir", default="/tmp/db", help="Database Directory", type=str)
@click.option("--features", default="aspath,bidirectionality,peeringdb,topological", help="Features to use during the computation", type=str)
@click.option("--overide", default=0, help="overide the results if existing", type=int)
@click.option("--input_list", default=None, help="AS-path list on the form as1 as2,as1 as2 as3-as1 as2,as1 as2 as3-etc..", type=str)
@click.option("--input_file", default=None, help="file with the links to read. Each line of the file must be on the form \"as1 as2,aspath\" . Basically, these files corresponds to the sampling files", type=str)
@click.option("--paper_eval", default=0, help="Perform the paper evaluation (with train test split", type=int)
@click.option("--fpr_weights", default=1, help="Sensitivity: a list of weights to apply to class 0 during learning. E.g., 1.2.3.4 will run the inference with four different weights.", type=str)
@click.option("--method", default="clusters", help="Load the righ method of sampling to do the inference with", type=str)
@click.option("--nb_days_training_data", default=300, help="Number of day for which to take the sampled links", type=int)


def run_inference_maker(date, db_dir, features, overide, input_list, input_file, paper_eval, fpr_weights, method, nb_days_training_data):

    if date is None:
        ut.err_msg("Please provide a date with option --date")
        exit(1)

    if input_list is None and input_file is None:
        ut.err_msg("Please provide any data source")
        exit(1)

    InfMake = InferenceMaker( \
        date, \
        db_dir, \
        sorted(features.split(",")), \
        overide, \
        list(map(lambda x:int(x), fpr_weights.split(','))), \
        method, \
        nb_days_training_data)
    InfMake.load_model()

    df = None
    if input_list:
        df = InfMake.load_from_string(input_list)

    if input_file:
        if df is None:
            df = InfMake.load_from_file(input_file)
        else:
            tmp_df = InfMake.load_from_file(input_file)
            if tmp_df is not None:
                df = pd.concat([df, tmp_df])


    if df is None or len(df.index) == 0:
        ut.err_msg("Please provide at least one good input")
        exit(1)
    

    InfMake.make_inference(df)
    print(InfMake.to_string())
    
if __name__ == "__main__":
    run_inference_maker()



    
