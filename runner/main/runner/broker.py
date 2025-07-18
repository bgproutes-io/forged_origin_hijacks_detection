from colorama import Fore, Style
import os
import json
import click
from datetime import datetime
from time import mktime
import sys
import random

import utils as ut
import run_features as rf



def print_prefix(msg, end="\n"):
    currentTime = datetime.now().strftime("%H:%M:%S")
    s = Fore.GREEN+Style.BRIGHT+"[broker.py ({})]: ".format(currentTime) +Style.NORMAL + msg + Fore.WHITE
    print(s, end=end, file=sys.stderr)


class RequestBroker:
    def __init__(self, date, db_dir, features, nb_days_training):
        self.date = date
        self.db_dir = db_dir
        self.results = []
        self.features = features
        self.nb_days_training = nb_days_training

    # Function used when the input links are in a file or in a PostgreSQL database
    def process_request(self, infile, store_results_in_db, idn=None):
        # Generate a random number used to uniqly identify the temporary files.
        if not idn:
            idn = random.randint(0, 1000000)

        df, feats = rf.run_features(None, self.date, self.db_dir, features=self.features, fn=infile, store_results_in_db=store_results_in_db, id=idn)
        res = rf.run_inference(df, self.db_dir, feats, self.date, self.nb_days_training, idn)

        for i in range(0, len(res.index)):
            line = dict()
            for feat in res.keys():
                line[feat] = str(res[feat].values[i])

            self.results.append(line.copy())

    # Function used when the input link is given directly in the CLI using the --input_link argument.
    def process_request_link(self, input_link, idn=None, peeringdb_clean=True):
        # Generate a random number used to uniqly identify the temporary files.
        if not idn:
            idn = random.randint(0, 1000000)

        asplist = []
        as1 = input_link.split(',')[0].split('-')[0]
        as2 = input_link.split(',')[0].split('-')[1]
        for aspath in input_link.split(',')[1:]:
            asplist.append((as1, as2, aspath))

        df, feats = rf.run_features(asplist, self.date, self.db_dir, features=self.features, fn=None, id=idn, suffix_name='_{}_{}'.format(as1, as2), peeringdb_clean=peeringdb_clean)
        res = rf.run_inference(df, self.db_dir, feats, self.date, self.nb_days_training, idn, docker_name='dfoh_inference_{}_{}'.format(as1, as2), fpr_weights="1")

        for i in range(0, len(res.index)):
            line = dict()
            for feat in res.keys():
                line[feat] = str(res[feat].values[i])

            self.results.append(line.copy())

    def to_json(self):
        return json.dumps(self.results)

    def to_text(self):
        s = ''
        for l in self.results:
            s += l['as1']+' '
            s += l['as2']+' '
            if 'asp' in l:
                s += l['asp']+' '
            else:
                s += 'None'+' '
            s += l['label']+' '
            s += l['proba']+' '
            s += l['sensitivity']+'\n'
        return s[:-1]

    def clear(self):
        self.results.clear()
        self.results = []


@click.command()
@click.option("--server", default=0, help="run broker in server mode", type=int)
@click.option("--date", default=None, help="Date to run the inference", type=str)
@click.option("--db_dir", default="/tmp/db", help="Database directory", type=str)
@click.option("--features", default="aspath,bidirectionality,peeringdb,topological", help="Features to use during the computation", type=str)
@click.option("--nb_days_training", default=300, help="Number of days prior the given date to for training", type=int)
@click.option("--store_results_in_db", default=False, help='If True, store results in PostrgreSQL db.', type=bool)
@click.option("--input_file", default=None, help="File where each line contains an AS link and the corresponding AS path (comma separated). Output is printed in outfile.", type=str)
@click.option("--input_link", default=None, help="String with link and as path in the form as1-as2-aspath1,aspath2,.... Output is printed in stdout", type=str)
@click.option("--peeringdb_clean", default=True, help="Boolean indicating whether temporary files (which are long to generate) should be cleaned up or not for the peeringdb features", type=bool)
@click.option("--outfile", default="results.txt", help="File where to store the results in text format (when using input_file)", type=str)


def run_broker(server, \
               date, \
               db_dir, \
               features, \
               nb_days_training, \
               store_results_in_db, \
               input_file, \
               input_link, \
               peeringdb_clean, \
               outfile):
    if server:
        app.run(port=80, host="0.0.0.0")
        exit(0)

    if date is None:
        ut.err_msg("please specify a date")
        exit(1)

    if input_link is None and (input_file is None or not os.path.exists(input_file)) and not store_results_in_db:
        ut.err_msg("please specify an input file or an input link or set store_results_in_db to True")
        exit(1)

    Broker = RequestBroker(date, db_dir, sorted(features.split(",")), nb_days_training)

    if store_results_in_db or (input_file and os.path.exists(input_file)):
        Broker.process_request(store_results_in_db=store_results_in_db, infile=input_file)

    with open(outfile, "w") as f:
        f.write(str(Broker.to_text()))

    if input_link is not None:
        Broker.process_request_link(input_link, peeringdb_clean=peeringdb_clean)
        print (str(Broker.to_text()))


if __name__ == "__main__":
    run_broker()



