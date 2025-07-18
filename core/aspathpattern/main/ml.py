from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import pickle
import bz2
import utils as ut
import prepare_dataset as prepd
import pandas as pd



####
# Function used to get the best parameters for the training of the
# random forest, regarding the training dataset.
#
# @param : X_train              Training dataset features
# @param : Y_train              Training dataset labels
#
####

def forest_GridSearchCV(X_train, Y_train):
    clf = RandomForestClassifier()


    param_grid = {
    'n_estimators': [50],
    'max_depth': [15],
    'min_weight_fraction_leaf': [0.],
    'min_samples_split': [2] 
    }


    grid_search = GridSearchCV(estimator = clf, param_grid = param_grid, cv = 4, n_jobs = 1)
    grid_search.fit(X_train, Y_train.values.ravel())

    return grid_search


####
# Function used to build and train the random forest.
#
# @param : X_train              Training dataset features
# @param : Y_train              Training dataset labels
#
####

def forest_build(X_train, Y_train, date, db_dir, metric, no_save, method):
    grid_search = forest_GridSearchCV(X_train, Y_train)

    clf = RandomForestClassifier(
    max_depth=grid_search.best_params_['max_depth'], \
    min_weight_fraction_leaf=grid_search.best_params_['min_weight_fraction_leaf'], \
    min_samples_split=grid_search.best_params_['min_samples_split'])

    clf.fit(X_train, Y_train.values.ravel())

    if not no_save:
        with bz2.BZ2File("{}/aspath_models_{}/{}_model_{}.pkl".format(db_dir, method, date, metric), "wb") as f:
            pickle.dump(clf, f)

    return clf



####
# Function used to build the internal decision tree for a given day
#
# @param db_dir         database directory
# @param date           date to build for
# @param metric         considered metric
####

def build_model_for_day(db_dir, date, metric, method, nbdays, no_save=True):

    # Get the last 30 days in an array
    all_dates = ut.get_the_last_n_days(date, nbdays)

    # Get the dataset for the current day
    X = prepd.get_dataset_for_one_day(db_dir, date, method, metrics=metric.split("&"))
    if X is None:
        ut.wrn_msg("No data for day {}".format(date))
    

    # For all nbdays days
    for d in all_dates:
        # Get the datset for this given day and append it to the
        # current dataset
        tmp_df = prepd.get_dataset_for_one_day(db_dir, d, method, metrics=metric.split("&"))
        if tmp_df is None:
            ut.wrn_msg("No data for day {}".format(d))
        
        else:
            if X is None:
                X = tmp_df
            else:
                X = pd.concat([X, tmp_df])
    
    if X is None or len(X.index) == 0:
        ut.err_msg("Unable to load any data, exit...")
        return None

    Y = X["label"]

    X = X.drop(columns=["as1", "as2", "label", "asp"])

    # Build the random forest
    return forest_build(X, Y, date, db_dir, metric, no_save, method)


####
# Function used to load the model when existing
#
# @param db_dir         database directory
# @param date           date to build for
# @param metric         considered metric
####

def load_model(date, db_dir, metric, method):
    with bz2.BZ2File("{}/aspath_models_{}/{}_model_{}.pkl".format(db_dir, method, date, metric), "rb") as f:
        clf = pickle.load(f)

    return clf


