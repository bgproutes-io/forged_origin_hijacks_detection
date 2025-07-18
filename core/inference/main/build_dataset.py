import utils as ut
import pandas as pd
import numpy as np



####
# Function used to load both negative and positive features
# for one specific day
####

def load_dataset_for_one_day(date, db_dir, features: list, method):
    df_pos = dict()
    df_neg = dict()
    df = dict()
    missing_data = False

    # We load all the required features: by default
    # aspath,topological,peeringdb and bgp
    feat_used = features.copy()

    # for each features load both opsitive and negative sampling
    for feat in features:
        fn_pos = "{}/features/positive/{}_{}/{}_positive.txt".format(db_dir, feat, method, date)
        fn_neg = "{}/features/negative/{}/{}_negative.txt".format(db_dir, feat, date)

        try:
            df_pos[feat] = pd.read_csv(fn_pos, sep=" ", index_col=False).fillna(1.0).drop_duplicates(subset=["as1", "as2"])
            df_pos[feat]["label"] = 1
            #print("For feat {} at date {}, positive dataset size is {}".format(feat, date, len(df_pos[feat].index)))
        except:
            df_pos[feat] = None

        
        try:
            df_neg[feat] = pd.read_csv(fn_neg, sep=" ", index_col=False).fillna(1.0).drop_duplicates(subset=["as1", "as2"])
            df_neg[feat]["label"] = 0
            #print("For feat {} at date {}, Negative dataset size is {}".format(feat, date, len(df_pos[feat].index)))
        except:
            df_neg[feat] = None

        if df_neg[feat] is None or df_pos[feat] is None:
            ut.wrn_msg("Unable to load training dataset for feature {} for day {}".format(feat, date))
            missing_data = True
            continue

        # Merge positive and negative sampling
        df[feat] = pd.concat([df_neg[feat], df_pos[feat]])

    if len(feat_used) == 0 or missing_data:
        return None

    feat_ref = feat_used[0]
    X = df[feat_ref]

    feat_used.remove(feat_ref)

    # merge all feature types to get the final dataset
    for feat in feat_used:
        X = X.merge(df[feat], how='inner', on=["as1", "as2", "label"])
        #X = my_merge(X, df[feat], on=["as1", "as2", "label"])

    #print("Final dataset for day {} is of size {}".format(date, len(X.index)))


    return X




def build_training_set(date, db_dir, features, method, nb_days=30):
    ut.wrn_msg("Loading training dataset for {} days before {}".format(nb_days, date))
    dates = ut.get_the_last_n_days(date, nb_days)

    X = load_dataset_for_one_day(date, db_dir, features, method)
    if X is None:
        ut.wrn_msg("Unable to load training dataset for day {}, skipped...".format(date))

    for d in dates:
        tmp_df = load_dataset_for_one_day(d, db_dir, features, method)

        if tmp_df is None:
            ut.wrn_msg("Unable to load training dataset for day {}, skipped...".format(d))
            continue
        
        if X is None:
            X = tmp_df
        else:
            X = pd.concat([X, tmp_df])

    return X


# def load_testing_dataset(date, db_dir, features: list, method):
#     df_pos = dict()
#     df_neg = dict()
#     df = dict()
#     missing_data = False

#     # We load all the required features: by default
#     # aspath,topological,peeringdb and bgp
#     feat_used = features.copy()

#     # for each features load both opsitive and negative sampling
#     for feat in features:
#         fn_pos = "{}/features/positive/{}_{}/{}_eval_tpr_fpr.txt".format(db_dir, feat, method, date)
#         fn_neg = "{}/features/negative/{}/{}_eval_tpr_fpr.txt".format(db_dir, feat, date)

#         try:
#             df_pos[feat] = pd.read_csv(fn_pos, sep=" ", index_col=False).fillna(1.0).drop_duplicates(subset=["as1", "as2"])
#             df_pos[feat]["label"] = 1
#             #print("For feat {} at date {}, positive dataset size is {}".format(feat, date, len(df_pos[feat].index)))
#         except:
#             df_pos[feat] = None

        
#         try:
#             df_neg[feat] = pd.read_csv(fn_neg, sep=" ", index_col=False).fillna(1.0).drop_duplicates(subset=["as1", "as2"])
#             df_neg[feat]["label"] = 0
#             #print("For feat {} at date {}, Negative dataset size is {}".format(feat, date, len(df_pos[feat].index)))
#         except:
#             df_neg[feat] = None

#         if df_neg[feat] is None or df_pos[feat] is None:
#             ut.wrn_msg("Unable to load testing dataset for feature {} for day {}".format(feat, date))
#             missing_data = True
#             continue

#         # Merge positive and negative sampling
#         df[feat] = pd.concat([df_neg[feat], df_pos[feat]])

#     if len(feat_used) == 0 or missing_data:
#         return None

#     feat_ref = feat_used[0]
#     X = df[feat_ref]

#     feat_used.remove(feat_ref)

#     # merge all feature types to get the final dataset
#     for feat in feat_used:
#         X = X.merge(df[feat], how='inner', on=["as1", "as2", "label"])
#         #X = my_merge(X, df[feat], on=["as1", "as2", "label"])

#     #print("Final dataset for day {} is of size {}".format(date, len(X.index)))

#     if X is None or len(X.index) == 0:
#         ut.err_msg("Unable to load any data, exit...")
#         return None

#     Y = X["label"]

#     X = X.drop(columns=["label"])


#     return X, Y
    