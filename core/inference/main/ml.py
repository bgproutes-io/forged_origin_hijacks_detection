from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
import pickle
import bz2
import utils as ut
import build_dataset as bds
import pandas as pd
from sklearn.model_selection import train_test_split



####
# Function used to get the best parameters for the training of the
# random forest, regarding the training dataset.
#
# @param : X_train              Training dataset features
# @param : Y_train              Training dataset labels
#
####

def forest_GridSearchCV(X_train, Y_train, fp_weight):
    clf = RandomForestClassifier()


    # param_grid = {
    # # 'criterion': ['gini', 'entropy'],
    # 'n_estimators': [20, 50, 100],
    # 'max_features': ['sqrt', "log2"],
    # 'max_depth': [3, 7, 15],
    # 'min_weight_fraction_leaf': [0., 0.1, 0.01],
    # 'min_samples_split': [2, 5, 10] 
    # }

    # param_grid = {
    # # 'criterion': ['gini', 'entropy'],
    # 'n_estimators': [20, 50, 100],
    # 'max_features': ['sqrt', "log2"],
    # 'max_depth': [15, 25, 30],
    # 'min_weight_fraction_leaf': [0., 0.1, 0.01],
    # 'min_samples_split': [2, 3, 4],
    # 'class_weight' : [{0:fp_weight, 1:1}]
    # }

    param_grid = {
    # 'criterion': ['gini', 'entropy'],
    'n_estimators': [100],
    'max_features': ['log2'],
    'max_depth': [12],
    'min_weight_fraction_leaf': [0.],
    'min_samples_split': [2] 
    }



    grid_search = GridSearchCV(estimator=clf, param_grid=param_grid, cv=2, n_jobs=1)
    grid_search.fit(X_train, Y_train.values.ravel())

    return grid_search


####
# Function used to build and train the random forest.
#
# @param : X_train              Training dataset features
# @param : Y_train              Training dataset labels
#
####

def forest_build(X_train, Y_train, date, db_dir, features, write=True, fp_weight=1):
    X_train, X_grid, Y_train, Y_grid = train_test_split(X_train, Y_train, train_size=0.9)

    # grid_search = forest_GridSearchCV(X_grid, Y_grid, fp_weight)

    # #print(grid_search.best_params_)

    # clf = RandomForestClassifier(
    # max_depth=grid_search.best_params_['max_depth'], \
    # min_weight_fraction_leaf=grid_search.best_params_['min_weight_fraction_leaf'], \
    # min_samples_split=grid_search.best_params_['min_samples_split'],
    # max_features=grid_search.best_params_['max_features'],
    # n_estimators=grid_search.best_params_['n_estimators'],
    # class_weight={0:fp_weight,1:1})

    clf = RandomForestClassifier(
    max_depth=12, \
    min_weight_fraction_leaf=0., \
    min_samples_split=2,
    max_features='log2',
    n_estimators=100,
    class_weight={0:fp_weight,1:1})

    #print("Fitting with a dataset of size {}".format(len(X_train.index)))
    clf.fit(X_train, Y_train.values.ravel())

    if len(features) == 4 or (len(features) == 3 and "aspath" not in features) and write:
        with bz2.BZ2File("{}/models/{}_model_{}_{}.pkl".format(db_dir, date, ",".join(sorted(features)), fp_weight), "wb") as f:
            pickle.dump(clf, f)

    return clf



####
# Function used to build the internal decision tree for a given day
#
# @param db_dir         database directory
# @param date           date to build for
# @param metric         considered metric
####



def build_model_for_day(db_dir, date, features, fp_weight, method, nb_days=30):

    X = bds.build_training_set(date, db_dir, features, method, nb_days=nb_days)
    if X is None or len(X.index) == 0:
        ut.err_msg("Unable to load any data, exit...")
        return None

    Y = X["label"]

    X = X.drop(columns=["as1", "as2", "label"])
   

    # Build the random forest
    return forest_build(X, Y, date, db_dir, features, fp_weight=fp_weight)


####
# Function used to build the testing and training set 
# for the paper evaluation
####

# def build_dataset_paper(db_dir, date, features, method, prop=0.3):
#     X = bds.build_training_set(date, db_dir, features, method)
    
#     if X is None or len(X.index) == 0:
#         ut.err_msg("Unable to load any data, exit...")
#         return None

#     Y = X["label"]

#     X = X.drop(columns=["label"])

#     return train_test_split(X, Y, test_size=prop)


####
# Function used to build the testing and training set 
# for the paper evaluation
####

# def build_dataset_paper_size(db_dir, date, features, method):
#     X = bds.build_training_set(date, db_dir, features, method)

#     if X is None:
#         ut.err_msg("Unable to load any data, exit... hello")
#         return None

#     if len(X.index) == 0:
#         ut.err_msg("Data size is 0, exit...")
#         return None

#     Y = X["label"]

#     X = X.drop(columns=["label"])
    
#     return X, Y

####
# Function used to load the model when existing
#
# @param db_dir         database directory
# @param date           date to build for
# @param metric         considered metric
####

def load_model(fn):
    with bz2.BZ2File(fn, "rb") as f:
        clf = pickle.load(f)

    return clf