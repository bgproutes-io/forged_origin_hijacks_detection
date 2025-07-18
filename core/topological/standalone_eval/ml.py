from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.inspection import permutation_importance



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
    'max_features': ['auto'],
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

def forest_build(X_train, Y_train):
    grid_search = forest_GridSearchCV(X_train, Y_train)

    clf = RandomForestClassifier(
    max_depth=grid_search.best_params_['max_depth'], \
    min_weight_fraction_leaf=grid_search.best_params_['min_weight_fraction_leaf'], \
    min_samples_split=grid_search.best_params_['min_samples_split'])

    clf.fit(X_train, Y_train.values.ravel())

    return clf


def build_dataset(df):
    Y = df["label"]
    X = df.drop(columns=["label"])

    return train_test_split(X, Y, test_size=0.30)



def forest_feature_importance(clf, feature_names, outfile):
    # Compute the feature importance (code mainly from sklearn doc).
    plt.clf()
    start_time = time.time()
    importances = clf.feature_importances_
    std = np.std([tree.feature_importances_ for tree in clf.estimators_], axis=0)
    elapsed_time = time.time() - start_time

    print(f"Elapsed time to compute the importances: {elapsed_time:.3f} seconds")

    forest_importances = pd.Series(importances, index=feature_names)

    fig = plt.figure(1, figsize=(8,6))
    dim = [0.10, 0.35, 0.79, 0.60]
    ax = plt.axes(dim)

    forest_importances.plot.bar(yerr=std, ax=ax)
    ax.set_title("Feature importances using MDI")
    ax.set_ylabel("Mean decrease in impurity")
    plt.savefig(outfile, format='pdf')

def forest_feature_importance_feature_permutation(clf, feature_names, X_test, Y_test, outfile):
    # Compute the feature importance (code mainly from sklearn doc).
    plt.clf()
    start_time = time.time()
    result = permutation_importance(
        clf, X_test, Y_test, n_repeats=10, random_state=42, n_jobs=10
    )
    elapsed_time = time.time() - start_time
    print(f"Elapsed time to compute the importances: {elapsed_time:.3f} seconds")

    forest_importances = pd.Series(result.importances_mean, index=feature_names)

    fig = plt.figure(1, figsize=(8,6))
    dim = [0.10, 0.35, 0.79, 0.60]
    ax = plt.axes(dim)

    forest_importances.plot.bar(yerr=result.importances_std, ax=ax)
    ax.set_title("Feature importances using permutation on full model")
    ax.set_ylabel("Mean accuracy decrease")
    plt.savefig(outfile, format='pdf')


