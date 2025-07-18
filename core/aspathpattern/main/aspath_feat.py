import ml
import utils as ut
import os
from colorama import Fore, Style
import time
import click
import sys
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor



def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN+Style.BRIGHT+"[aspath_feat.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)

    
class ASPathFeatureComputer:
    def __init__(self, date, db_dir, metrics, override, method, nbdays):
        self.date = date            # Date of the model
        self.db_dir = db_dir        # Database directory
        self.model = dict()         # ml model dictionnary, one per metric
        self.metrics = metrics      # All considered metrics
        self.results = []           # Inference results
        self.override = override
        self.method = method
        self.nbdays = nbdays        # Number of days to consider to train the inference model.

        self.metrics.append("&".join(sorted(metrics)))

        for m in metrics:
            self.model[m] = None


        ml.ut.load_all_degrees(date, db_dir)
        ml.ut.load_all_ascones(date, db_dir)

        try:
            ut.create_directory("{}/features".format(self.db_dir))
            ut.create_directory("{}/features/positive".format(self.db_dir))
            ut.create_directory("{}/features/negative".format(self.db_dir))
            ut.create_directory("{}/features/positive/aspath_{}".format(self.db_dir, self.method))
            ut.create_directory("{}/features/negative/aspath".format(self.db_dir))
            ut.create_directory("{}/aspath_models_{}".format(self.db_dir, self.method))
        except FileNotFoundError:
            ut.err_msg("Database directories could not be created, are you sure your database is located at \"{}\" ?".format(self.db_dir))
            exit(1)

    
    # Load one ml model for a specific day
    def load_model(self, metric, ov, daily_sampling):
        model_file = "{}/aspath_models_{}/{}_model_{}.pkl".format(self.db_dir, self.method, self.date, metric)

        # If the model exists, just load it
        if os.path.exists(model_file) and not ov:

            start = time.time()
            self.model[metric] = ml.load_model(self.date, self.db_dir, metric, self.method)
            stop = time.time() - start

            print_prefix("Model has been found in {}, loaded in {:.4f} s".format(model_file, stop))

        # Else, build it
        else:
            print_prefix("Model not found on local disk, needs to be computed...")

            start = time.time()
            self.model[metric] = ml.build_model_for_day(self.db_dir, self.date, metric, self.method, self.nbdays, no_save=False)
            stop = time.time() - start

            if self.model[metric] is None:
                ut.err_msg("Unable to build model for day {}".format(self.date))
                exit(1)

            print_prefix("Model has been build in {:.4f} s".format(stop))


    # load models for all the metrics
    def load_models(self, ov, daily_sampling):
        for m in self.metrics:
            self.load_model(m, ov, daily_sampling)

    
    # Infer the probability of an aspath to be malicious,
    # for all aspath in the provided list
    def asp_inference(self, asp_list, daily_sampling=False):
        preds = dict()
        X_test = dict()

        for m in self.metrics:
            X_test[m] = ml.prepd.asp_list_to_dataset(asp_list, metrics=m.split("&"))

            preds[m] = self.model[m].predict_proba(X_test[m].drop(columns=["as1", "as2", "asp"]))

        ref_m = self.metrics[0]
        for i in range(0, len(X_test[ref_m])):
            ok = True

            # Check if the ASN are consistent for all the metrics
            if len(self.metrics) > 1:
                as1 = X_test[ref_m]["as1"].values[i]
                as2 = X_test[ref_m]["as2"].values[i]

                for m in self.metrics[1:]:
                    as1_tmp = X_test[m]["as1"].values[i]
                    as2_tmp = X_test[m]["as2"].values[i]

                    if as1_tmp != as1 or as2_tmp != as2:
                        ut.wrn_msg("Inconsistency for as1 as2 between {} and {} at line {}, skipped...".format(ref_m, m, i))
                        ok = False

            # End of check
            if ok:
                new_line = dict()
                new_line["as1"] = X_test[ref_m]["as1"].values[i]
                new_line["as2"] = X_test[ref_m]["as2"].values[i]

                # Only store the AS-path if it is a request from broker
                if not daily_sampling:
                    new_line["asp"] = X_test[ref_m]["asp"].values[i]

                for m in self.metrics:
                    new_line[m] = preds[m][i][1]

                self.results.append(new_line.copy())

    
    # Transform the results into a CSV string format
    def to_string(self, label):
        s = ""
        if len(self.results) < 1:
            return s

        keys = list(self.results[0].keys())

        if label is not None:
            keys.append("label")

        s += " ".join(keys)
        s += "\n"

        for line in self.results:
            str_line = [str(v) for v in list(line.values())]

            if label is not None:
                str_line.append(str(label))

            s+= " ".join(str_line)
            s += "\n"

        return s


    def clear(self):
        self.results.clear()


    # Build the daily sampling
    def daily_sampling(self):
        fn_pos = "{}/sampling/positive/sampling_{}/{}_positive.txt".format(self.db_dir, self.method, self.date)
        fn_neg = "{}/sampling/negative/sampling/{}_negative.txt".format(self.db_dir, self.date)

        start = time.time()

        # Save the results in the corresponding files
        fn_pos_feat = "{}/features/positive/aspath_{}/{}_positive.txt".format(self.db_dir, self.method, self.date)
        if os.path.exists(fn_pos_feat) and not self.override:
            print_prefix("Positive Sampling for day {} already exists, skipped...".format(self.date))
        else:
            asplist = ml.ut.file_to_aspaths_list(fn_pos)

            self.asp_inference(asplist, daily_sampling=True)
            with open(fn_pos_feat, "w") as f:
                f.write(self.to_string(None))

        self.clear()
        
        fn_neg_feat = "{}/features/negative/aspath/{}_negative.txt".format(self.db_dir, self.date)
        if os.path.exists(fn_neg_feat) and not self.override:
            print_prefix("Negative Sampling for day {} already exists, skipped...".format(self.date))
        else:
            asplist = ml.ut.file_to_aspaths_list(fn_neg)

            self.asp_inference(asplist, daily_sampling=True)
            with open(fn_neg_feat, "w") as f:
                f.write(self.to_string(None))

        tick = time.time() - start

        print_prefix("Sampling for day {} took {:.4f} s".format(self.date, tick))


def aspath_feat_aux(date, db_dir, met, override, overide_model, method):
    aspfc = ASPathFeatureComputer(date, db_dir, met, override, method)
    aspfc.load_models(overide_model, 1)

    aspfc.daily_sampling()



@click.command()
@click.option("--date", help="Date of the link appearance, used to load the right topology", type=str)
@click.option("--outfile", default=None, help="file where to write the results of the feature computation", type=str)
@click.option("--db_dir", default="/tmp/db", help="Database Directory", type=str)
@click.option("--metrics", default="degree,cone", help="Features to use during the computation", type=str)
@click.option("--aspath_file", default=None, help="file with the links to read. Each line of the file must be on the form \"as1 as2,aspath\" . Basically, these files corresponds to the sampling files", type=str)
@click.option('--store_results_in_db', is_flag=True, help='If set, store the results in the PostgreSQL database.')
@click.option("--label", default=None, help="Label to assign to each link", type=int)
@click.option("--daily_sampling", default=0, help="Builds the daily sampling, in terms of positive and negative samples. Should be passed with option --date", type=int)
@click.option("--overide_model", default=0, help="Set to 1 if you want to verride the existing models", type=int)
# Ex 134896 9583,34224 6939 9583 134896-133322 7713,7713 133322 24549
@click.option("--aspath_list", default=None, help="AS-path list on the form as1 as2,as1 as2 as3-as1 as2,as1 as2 as3-etc..", type=str)
@click.option("--override", default=0, help="override the results if existing", type=int)
@click.option("--end_date", default=None, help="End date of bunch", type=str)
@click.option("--nb_threads", default=2, help="Number of threads to use to compute the aspath features", type=int)
@click.option("--method", default="clusters", help="Method used fo the positive sampling", type=str)
@click.option("--nbdays", default=300, help="Number of days to consider when building the model", type=int)


def run_orchestrator(date, \
    outfile, \
    db_dir, \
    metrics, \
    store_results_in_db, \
    aspath_file, \
    label, \
    daily_sampling, \
    overide_model, \
    aspath_list, \
    override, \
    end_date, \
    nb_threads, \
    method, \
    nbdays):
    # Exit if no date are provided
    if date is None:
        ut.err_msg("Please enter a date with option --date")
        exit(1)  
    
    met = metrics.split(",")

    # Exit if one of the metric is unavailable
    for m in met:
        if m not in ["degree", "cone"]:
            ut.err_msg("metric {} is not available, must be choosen between [degree , cone]".format(m))
            exit(1)

    if end_date is not None:
        all_dates = ut.get_all_dates(date, end_date)
        print(all_dates)
        all_procs = []

        with ProcessPoolExecutor(max_workers=nb_threads) as exec:
            for d in all_dates:
                all_procs.append(exec.submit(aspath_feat_aux, d, db_dir, met, override, overide_model, method))

            for p in all_procs:
                p.result()

        exit(0)

    # Exit if not data source is provided
    if not daily_sampling and aspath_file is None and aspath_list is None and not store_results_in_db:
        ut.err_msg("Please enter a data source with option --daily_sampling=1, --aspath_file, --aspath_list or set --store_results_in_db")
        exit(1)

    aspfc = ASPathFeatureComputer(date, db_dir, met, override, method, nbdays)
    aspfc.load_models(overide_model, daily_sampling)

    if daily_sampling:
        aspfc.daily_sampling()
        exit(0)

    asplist = []

    if store_results_in_db:
        asplist = ml.ut.extract_aspath_list_from_db(date)
    
    # Transform the file into a list of AS-path
    elif aspath_file:
        asplist = ml.ut.file_to_aspaths_list(aspath_file)

    # Append the aspath_list
    elif aspath_list:
        all_asp = aspath_list.split("-")
        for asp_link in all_asp:
            as1 = asp_link.split(",")[0].split(" ")[0]
            as2 = asp_link.split(",")[0].split(" ")[1]

            if int(as1) > int(as2):
                as1, as2 = as2, as1

            asp = asp_link.split(",")[1]

            asplist.append((as1, as2, asp))

    if len(asplist) < 1:
        ut.err_msg("File with aspath list may be empty")
        exit(1)

    aspfc.asp_inference(asplist)

    if outfile is None:
        print(aspfc.to_string(label), end="")
    else:
        with open(outfile, "w") as f:
            f.write(aspfc.to_string(label))

    


if __name__ == "__main__":
    run_orchestrator()

    

        
    