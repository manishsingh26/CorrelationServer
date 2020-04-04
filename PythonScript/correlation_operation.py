import csv
from os.path import join, commonprefix
import ipaddress
from scipy.stats import linregress
import numpy as np

np.warnings.filterwarnings('ignore')
inf = float("inf")


def create_dict(file_name, dest, key_inds, data_inds, file_index=0):
    """Convert csv file to corresponding pivot table

    Arguments:
        file_name {str} -- path to input file
        dest {str} -- path to output file. If file already exists, new entries will be appended onto it. rerunning the script shall lead to double entries
        key_inds {List[int]} -- List of indices to columns which contain the key values(multiple values will lead to cross product taken). These will be the columns of the pivot table. If data type is identified as an ipv4 address, only first 24 bits will be used.
        data_inds {List[ind]} -- List of indices to columns which contain the data values(multiple values will lead to multiple columns). These vill become the cells of the pivot table.

    Keyword Arguments:
        file_index {int} -- index of the column containing instance identifier. This will become the rows of the pivot table. (default: {0})
    """

    new_data = []
    old_data = {}
    header = None
    infile = csv.reader(open(file_name))
    header = next(infile, [])
    for row in infile:
        new_data.append(row)
    hfields = len(key_inds)
    titles = ["K_" + header[i] for i in key_inds]
    try:
        hfields = len(key_inds)
        infile = csv.reader(open(dest))
        for row in infile:
            if row[0][:2] == "K_":
                titles = row
            else:
                old_data[tuple(row[:hfields])] = row[hfields:]
    except:
        # File probably does not exist
        pass

    # we assume that all files encountered in the current csv is new
    # we find out what new files we will be encountering
    # file_index = int(file_index)
    # print(file_index)
    unique_names = set([p[file_index] for p in new_data])
    back_map = {}
    for k in unique_names:
        back_map[k] = len(titles) - hfields
        titles.append(k)
    for d in old_data:
        old_data[d].extend(["|||".join(["inf" for x in data_inds]) for i in range(len(unique_names))])
    for k in new_data:
        cur_key = convert([k[x] for x in key_inds])

        if cur_key in old_data:
            old_data[cur_key][back_map[k[file_index]]] = "|||".join([k[x] for x in data_inds])
        else:
            old_data[cur_key] = ["|||".join(["inf" for x in data_inds]) for i in range(len(titles) - hfields)]
            old_data[cur_key][back_map[k[file_index]]] = "|||".join([k[x] for x in data_inds])

    outfile = csv.writer(open(dest, "w", newline=''))
    outfile.writerow(titles)
    for key in old_data:
        outfile.writerow(list(key) + old_data[key])


def convert(inp):
    out = []
    for k in inp:
        try:
            out.append(ip(k)[:24])
        except:
            out.append(k)
    return tuple(out)


def split(file_name, dest, key_inds):
    header = None
    infile = csv.reader(open(file_name))
    rows = []
    for row in infile:
        if row[1] == "File Name":
            header = row
        else:
            rows.append(row)
    k = set([tuple([r[x] for x in key_inds]) for r in rows])
    outfiles = {}
    for ent in k:
        outfiles[ent] = csv.writer(open(join(dest, str(ent) + ".csv"), "w", newline=''))
        outfiles[ent].writerow(header)
    for row in rows:
        outfiles[tuple([row[x] for x in key_inds])].writerow(row)


def perc(inp):
    inp = inp.strip()
    if inp[-1] == "%":
        return float(inp[:-1])
    else:
        raise ValueError("Not a valid percentage")


def ip(inp):
    inp = inp.strip()
    if inp.count(".") == 3:
        if "/" not in inp:
            inp += ".32"
        else:
            inp.replace("/", ".")
        ret = [int(x) for x in inp.split(".")]
        out = "{:08b}{:08b}{:08b}{:08b}".format(ret[0], ret[1], ret[2], ret[3])
        return out[:ret[4]]
    raise ValueError("Not a valid IP")


def get_type(col, thresh=0.9):
    tot = float(sum([i != "inf" for i in col]))
    # simple float
    succ = 0
    out = []
    for e in col:
        if e != "inf":
            try:
                out.append(float(e))
                succ += 1
            except:
                out.append(inf)
        else:
            out.append(inf)
    if succ / tot > thresh:
        return (0, out)

    # percentage
    succ = 0
    out = []
    for e in col:
        if e != "inf":
            try:
                out.append(perc(e))
                succ += 1
            except:
                out.append(inf)
        else:
            out.append(inf)
    if succ / tot > thresh:
        return (1, out)

    # number with units
    try:
        sec = [k.strip().split()[-1] for k in col if k.strip().count(" ") == 1]
        if (max([sec.count(k) for k in set(sec)])) / tot > thresh:
            unit = max(set(sec), key=sec.count)
            return 2, get_type(
                [k.strip().split()[0] if (k == "inf" or k.strip().split()[1] == unit) else "inf" for k in col], thresh)[
                1]
    except:
        pass
        # ip address
    succ = 0
    out = []
    for e in col:
        if e != "inf":
            try:
                out.append(ip(e))
                succ += 1
            except:
                out.append(inf)
        else:
            out.append(inf)
    if succ / tot > thresh:
        return (3, out)
    return -1, col


def get_counts(l):
    counts = {}
    for a in l:
        if a in counts:
            counts[a] += 1
        else:
            counts[a] = 1
    return counts


def detect_correlation(c1, c2, tp_c1, tp_c2, cat_thresh=0.3, cat_margin=3, min_corr=0.75):
    # Types covered:
    # CC_Pos(a,b,conf)      :a and b occur together in their respective columns
    # CC_As_0(a,b,conf)     :b occurs whenever a occurs
    # CC_As_1(a,b,conf)     :a occurs whenever b occurs
    # CC_Neg(a,b,conf)      :though a and b are fairly frequent in their respective columns, they rarely occur together
    # CI_Prf(a,b,conf)      :for category a, all ips have common prefix b
    # IC_Prf(a,b,conf)      :same as  CI_Prf, column order reversed
    # II_100_80_40(a,b,c)   :all pairs have atleast a common bits, 80% have b common bits, 60% have c common bits
    # FF_LinReg(m,c,r)      :the two columns are related as y=mx+c with an r-squared score of r

    if type(cat_margin) is float and cat_margin < 1.0:
        cat_margin = int(cat_margin * len(c1))
    rels = []
    # Both Categorical
    # if we are treating an ip as a categorical variable, we would probably want to use the subnet
    c1_counts = get_counts(c1)
    c2_counts = get_counts(c2)
    cross_counts = get_counts(zip(c1, c2))
    # Positive correlations
    corr = [((cross_counts[z]) / ((c1_counts[z[0]] * c2_counts[z[1]]) ** 0.5 + cat_margin), z[0], z[1]) for z in
            cross_counts]
    for i in corr:
        if str(i[2]).strip() != "inf" and str(i[1]).strip() != "inf":
            if i[0] > min_corr:
                rels.append(("CC_Pos", i[1], i[2], i[0]))
            else:
                if (cross_counts[(i[1], i[2])]) / (c1_counts[i[1]] + 1.0 * cat_margin) > min_corr:
                    rels.append(
                        ("CC_As_0", i[1], i[2], (cross_counts[(i[1], i[2])]) / (c1_counts[i[1]] + 1.0 * cat_margin)))
                elif (cross_counts[(i[1], i[2])]) / (c2_counts[i[2]] + 1.0 * cat_margin) > min_corr:
                    rels.append(
                        ("CC_As_1", i[1], i[2], (cross_counts[(i[1], i[2])]) / (c2_counts[i[2]] + 1.0 * cat_margin)))

    # Negative correlations
    def get(p, q):
        if (p, q) in cross_counts:
            return cross_counts[(p, q)]
        else:
            return 0

    thresh = int(len(c1) * cat_thresh)
    c1_maj = [k for k in c1_counts if c1_counts[k] > thresh and k != "inf"]
    c2_maj = [k for k in c2_counts if c2_counts[k] > thresh and k != "inf"]
    negs = [((get(p, q)) / ((c1_counts[p] * c2_counts[q]) ** 0.5), p, q) for p in c1_maj for q in c2_maj]
    for i in negs:
        if i[0] < (1 - min_corr):
            rels.append(("CC_Neg", i[1], i[2], 1 - i[0]))

    rev = False
    if tp_c2 == -1 and tp_c1 != -1:
        tp_c1, tp_c2, c1, c2 = tp_c2, tp_c1, c2, c1
        rev = True
    # Categorical and IP
    if tp_c1 == -1 and tp_c2 == 3:
        # relations considering IP as cat have already been calculated
        combs = {}
        for i in zip(c1, c2):
            if i[1] != "inf" and i[0] != "inf":
                if i[0] in combs:
                    combs[i[0]].append(i[1])
                else:
                    combs[i[0]] = [i[1]]
        if rev == True:
            for k in combs:
                rels.append(("CI_Prf", k, commonprefix(combs[k]), len(combs[k]) / (cat_thresh * len(c1))))
        else:
            for k in combs:
                rels.append(("IC_Prf", k, commonprefix(combs[k]), len(combs[k]) / (cat_thresh * len(c1))))

    # Both IP
    if tp_c1 == 3 and tp_c2 == 3:
        max_match = sorted([len(commonprefix([p, q])) for p, q in zip(c1, c2)], reverse=True)
        rels.append(("II_100_80_60", max_match[-1], max_match[int(len(c1) * 0.8)], max_match[int(len(c1) * 0.6)]))
        # check for categorical matching in class C:
        class_C = detect_correlation([c[:-8] for c in c1 if len(c) > 8], [c[:-8] for c in c2 if len(c) > 8], -1, -1)
        for j in class_C:
            rels.append(("IC" + j[0][2:], j[1], j[2], j[3]))

    # both float:
    if tp_c1 in [0, 1, 2] and tp_c2 in [0, 1, 2]:
        # remove inf values
        zipped = [k for k in zip(c1, c2) if k[0] != float("inf") and k[1] != float("inf")]
        if len(zipped) > 0:
            slope, intercept, r_value, _, _ = linregress([k[0] for k in zipped], [k[1] for k in zipped])
            if str(intercept) != "nan" and r_value > min_corr ** 0.5:
                rels.append(("FF_LinReg", slope, intercept, r_value * r_value))

    # categorical and float

    # IP and float
    return rels


def load_data(file_name, pprint=False):
    res = {}
    infile = csv.reader(open(file_name))
    heads = []
    for row in infile:
        if row[0][:2] != "K_":
            value = zip(*([k.split("|||") for k in row[len(heads):]]))
            value = list(value)
            for i in range(len(value)):
                res[tuple(row[:len(heads)] + [i])] = value[i]
        else:
            for el in row:
                if el[:2] == "K_":
                    heads.append(el)
                else:
                    break
    if pprint:
        for k in res:
            # print (k, "\t", len(res[k]))
            pass
    return res, heads + ["Data Index"]


def get_all_corr(file_name, dest_folder, index_time):
    cat_thresh = 0.3
    corr_thresh = 1.1
    cat_margin = 3
    min_corr = 0.75

    """Get all golden values and pairwise correlations between all columns present. In case multiple data indices were used to generate the pivot table, multiple columns in the same key will be labelled as 0,1,... according to their order in the data_indices list

    Arguments:
        file_name {str} -- path to pivot table
        dest_folder {str} -- path to folder where output is to be stored. two csv files "golden_Flexi-BSC_switch-working-state_20-09-2018@17-51-24.56.csv" and "correlations_Flexi-BSC_switch-working-state_20-09-2018@17-51-24.56..csv" will be generated

    Keyword Arguments:
        cat_thresh {float} -- Frequency threshold below which golden values and negative correlations will be ignored (default: {0.3})
        corr_thresh {float} -- Max frequency above which column will be ignored for pairwise correlations (default: {1.1})
        cat_margin {int} -- No of elements to be added for smmothing out correlations (default: {3})
        min_corr {float} -- Minimum confidence to output correlation (default: {0.75})
    """

    res, heads = load_data(file_name)
    k = list(res.keys())
    v = [get_type(res[i]) for i in k]
    refined_v = []
    refined_k = []
    # print("Finding golden values(frequency:=value):\n")
    gold_file = csv.writer(open(join(dest_folder, "golden_.csv".replace(".csv", index_time + ".csv")), "w", newline=''))
    gold_file.writerow(heads + ["Golden value"] + ["conf_prob"])

    for i in range(len(k)):
        # print("\n",k[i])
        cur_row = list(k[i])
        d = get_counts(res[k[i]])
        ln = 1.0 * len(v[i][1])

        try:
            ln = ln - d["inf"]
        except:
            pass
        mval = 0
        for item in d:
            if str(item) != "inf" and d[item] / ln > cat_thresh:
                # print("{:3f}:={}".format(d[item]/ln,item))
                cur_row.append(item)
                cur_row.append(d[item] / ln)
                if d[item] > mval:
                    mval = d[item]
        if mval < corr_thresh * ln:
            refined_v.append(v[i])
            refined_k.append(k[i])

        gold_file.writerow(cur_row)
    v = refined_v
    # print("\n\nNow finding correlations:\n")
    corr_file = csv.writer(
        open(join(dest_folder, "correlations_.csv".replace(".csv", index_time + ".csv")), "w", newline=''))
    corr_file.writerow(
        [x + "_1" for x in heads] + [y + "_2" for y in heads] + ["Correlation type", "Parameter 1", "Parameter 2",
                                                                 "Parameter 3"])
    k = refined_k

    for i in range(len(k) - 1):
        for j in range(i + 1, len(k)):
            try:
                d = detect_correlation(v[i][1], v[j][1], v[i][0], v[j][0], cat_thresh=cat_thresh)

                if len(d) > 0:
                    corr_file.writerows([list(k[i]) + list(k[j]) + list(m) for m in d])
            except:
                pass
                # print(k[i],"\t\t",k[j])
                # print(d,"\n")
    return join(dest_folder, "correlations_.csv".replace(".csv", index_time + ".csv")), join(dest_folder,
                                                                                             "golden_.csv".replace(
                                                                                                 ".csv",
                                                                                                 index_time + ".csv"))

