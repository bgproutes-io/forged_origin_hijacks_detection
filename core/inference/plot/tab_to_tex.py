import heatmap


date = "eval"
db_dir = "/root/type1_main/setup/db"
features = [["topological"], ["aspath","bidirectionality"], ["peeringdb"], ["topological", "peeringdb"], ["aspath","bidirectionality", "peeringdb"], ["aspath","bidirectionality", "topological"], sorted("aspath,bidirectionality,peeringdb,topological".split(","))]


link_types = {
    0: "Stubs",
    1: "Transit/IXP/CDN-1",
    2: "Transit/IXP/CDN-2",
    3: "Transit/IXP/CDN-3",
    4: "Transit/IXP/CDN-4",
    5: "Highly Connected",
    6: "Large Customer Cone",
    7: "Tier-One"
}


def build_column_name(feats):
    if len(feats) == 4:
        return "All Features"
    
    base = "_".join(sorted(feats))
    base = base.replace("aspath_bidirectionality", "BGP")
    base = base.replace("peeringdb", "Peering")
    base = base.replace("topological", "Topological")

    base = base.replace("Topological", "Topo")

    return base.replace("_", " + ")


def load_data():
    tables_tpr = dict()
    tables_fpr = dict()

    for feats in features:
        key = "_".join(sorted(feats))
        tables_tpr[key] = heatmap.compute_table_rate(date, db_dir, "../main/tmp/tp_{}.txt".format(key), "../main/tmp/fn_{}.txt".format(key))
        tables_fpr[key] = heatmap.compute_table_rate(date, db_dir, "../main/tmp/fp_{}.txt".format(key), "../main/tmp/tn_{}.txt".format(key))
        print("{} loaded".format(key))

    return tables_fpr, tables_tpr


def count_line_in_file(fn):
    nbLine = 0
    with open(fn, 'r') as f:
        for line in f:
            nbLine += 1

    return nbLine


def get_tpr_fpr_tot_one_cat(feats):
    key = "_".join(sorted(feats))
    TP = count_line_in_file("../main/tmp/tp_{}.txt".format(key))
    FP = count_line_in_file("../main/tmp/fp_{}.txt".format(key))
    TN = count_line_in_file("../main/tmp/tn_{}.txt".format(key))
    FN = count_line_in_file("../main/tmp/fn_{}.txt".format(key))

    return key, (TP / (TP + FN)) * 100, (FP / (TN + FP)) * 100




def to_multirow(v):
    s = "\\mr{"
    s += v
    s += "}"
    return s

def to_multicol(v, rows=2):
    s = "\\multicolumn{"+str(rows)+"}{c}{"
    s += v
    s += "}"
    return s

def to_textbf(v):
    s = "\\textbf{"
    s += v
    s += "}"
    return s


def to_underline(v):
    s = "\\underline{"
    s += v
    s += "}"
    return s


def build_header():
    s = " &"
    #s = to_multirow("Link Type")
    for feats in features:
        s += " & "
        s += to_multicol(build_column_name(feats))

    s += " \\\\\n"
    s += "\cmidrule(r){3-4}\n"
    s += "\cmidrule(r){5-6}\n"
    s += "\cmidrule(r){7-8}\n"
    s += "\cmidrule(r){9-10}\n"
    s += "\cmidrule(r){11-12}\n"
    s += "\cmidrule(r){13-14}\n"
    s += "\cmidrule(r){15-16}\n"
    s += "\\vspace{0.2cm}\n"
    s += " &"
    for feats in features:
        s += " & "
        s += "TPR & FPR"

    s += "\\\\\\toprule\\vspace{0.2cm}\n"

    return s


def build_one_line(tables_tpr, tables_fpr, t1, t2):
    s = " & "
    s += link_types[t1]
    s += " -"

    for feats in features:
        key = "_".join(sorted(feats))
        s += " & "
        val = tables_tpr[key][t1][t2] * 100
        val = "{:.1f} \\%".format(val)
        if len(feats) == 4:
            s += to_multirow(to_textbf(val))
        else:
            s += to_multirow(val)

        s += " & "
        val = tables_fpr[key][t1][t2] * 100
        val = "{:.1f} \\%".format(val)
        if len(feats) == 4:
            s += to_multirow(to_textbf(val))
        else:
            s += to_multirow(val)

    s += " \\\\\n & "

    s += link_types[t2]

    for feats in features:
        s += " & &"
    
    s += " \\\\\\cmidrule{2-16}\\vspace{0.2cm}\n"

    return s


def get_last_line():
    s = " & "
    s += to_multirow(to_textbf("All types of Links"))

    for feats in features:
        s += " & "
        key, tpr, fpr = get_tpr_fpr_tot_one_cat(feats)
        val = tpr
        val = to_underline("{:.1f} \\%".format(val))
        if len(feats) == 4:
            s += to_multirow(to_textbf(val))
        else:
            s += to_multirow(val)

        s += " & "
        val = fpr
        val = to_underline("{:.1f} \\%".format(val))
        if len(feats) == 4:
            s += to_multirow(to_textbf(val))
        else:
            s += to_multirow(val)

    s += " \\\\\n & "

    for feats in features:
        s += " & &"
    
    s += " \\\\\\toprule\\vspace{0.2cm}\n"

    return s



def build_full_table():
    tables_fpr, tables_tpr = load_data()

    s = build_header()

    print(len(tables_fpr))

    for i in range(0, len(tables_fpr) + 1):
        for j in range(i, len(tables_fpr) + 1):
            s += build_one_line(tables_tpr, tables_fpr, i, j)
            print("Done for {} -> {}".format(i, j))

    s += get_last_line()
    
    return s


if __name__ == "__main__":
    with open("test.tex", "w") as f:
        f.write(build_full_table())
