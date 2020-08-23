import numpy as np
from sklearn.impute import KNNImputer


def impute_by_average(data, data_types):
    sum_something = [int(float(x)) if x != 'NULL' else 0 for x in data[0]]
    print(sum_something)
    row_length = len(data[0])
    no_of_rows = len(data)
    for i, val in enumerate(data[0]):
        for j in range(1, no_of_rows):
            if data[j][i] != 'NULL':
                sum_something[i] += int(float(data[j][i]))
    print([float(value/no_of_rows) for value in sum_something])
    return [float(value/no_of_rows) for value in sum_something]


def impute_by_zeroing(data, data_types):
    zeroed = []
    for i, val in enumerate(data_types):
        if val == 'string':
            zeroed.append('')
        else:
            zeroed.append(0)

    return zeroed


def impute_by_most_frequent(data, data_types):
    frequency_dict = {}
    row_length = len(data[0])
    no_of_rows = len(data)
    most_frequent = []
    for i in range(row_length):
        for j in range(no_of_rows):
            if data[j][i] != 'NULL':
                if i in frequency_dict.keys():
                    if data[j][i] in frequency_dict[i].keys():
                        frequency_dict[i][data[j][i]] += 1
                    else:
                        frequency_dict[i][data[j][i]] = 0
                else:
                    frequency_dict[i] = {data[j][i]: 0}
    for i in range(row_length):
        if i not in frequency_dict.keys():
            frequency_dict[i] = {'NULL': 0}
    for key in frequency_dict.keys():
        keys_in_key = list(frequency_dict[key].keys())
        values_in_keys = list(frequency_dict[key].values())
        most_frequent.append(
            keys_in_key[values_in_keys.index(max(values_in_keys))])

    return most_frequent


def impute_by_knn(data, data_types):
    nan = np.nan
    X = [[1, 2, nan], [3, 4, 3], [nan, 6, 5], [8, 8, 7]]
    imputer = KNNImputer(n_neighbors=2, weights="uniform")
    print(imputer.fit_transform(X))
