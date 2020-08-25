import numpy as np
from sklearn.impute import KNNImputer
from sklearn.impute import SimpleImputer
#import datawig


def impute_by_average(data, data_types):
    sum_something = [int(float(x)) if x != 'NULL' else 0 for x in data[0]]
    row_length = len(data[0])
    no_of_rows = len(data)
    for i, val in enumerate(data[0]):
        for j in range(1, no_of_rows):
            if data[j][i] != 'NULL':	
                if data_types[i] == 'int':
                    sum_something[i] += int(float(data[j][i]))
                else:
                    sum_something[i] += float(data[j][i])
    return [float(value/no_of_rows) if data_types[i] == 'float' else int(value/no_of_rows) for i,value in enumerate(sum_something)]


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
    row_length = len(data[0])
    no_of_rows = len(data)

    for i in range(row_length):
        for j in range(no_of_rows):
            if data[j][i] == 'NULL':
                data[j][i] = np.nan

    imputer = KNNImputer(n_neighbors=2, weights="uniform")
    final_data = list(imputer.fit_transform(data))
    for i in range(len(final_data[0])):
        for j in range (len(final_data)):
            if data_types[i] == 'float':
                final_data[j][i] = str(float(final_data[j][i]))
            else:
                final_data[j][i] = str(int(float(final_data[j][i])))

    return final_data

#def impute_by_ml(data, data_types):
#   
#    # impute missing values
#    df_with_missing_imputed = datawig.SimpleImputer.complete(data)
#    return df_with_missing_imputed

