import sys
import os
import shutil
from texttable import Texttable
import re
import numpy as np

line_length = 40
global_variables = {
    "time_quality_factor": {"min": 0.0,
                            "max": 1.0,
                            "default": 0.5,
                            "current_value": 0.5},
    "impute_method": {"allowed_values": ["average", "frequent", "no", "zeroing", "auto", "knn", "ml"],
                      "default": "knn",
                      "current_value": "knn"},
    "best_effort": {"allowed_values": ["1", "0"],
                    "default": "0",
                    "current_value": "0"},
    "display_percentage": {"allowed_values": ["1", "0"],
                           "default": "0",
                           "current_value": "0"},
    "print": {"allowed_values": ["1", "0"],
              "default": "1",
              "current_value": "1"},
    "include_null": {"allowed_values": ["1", "0"],
                     "default": "1",
                     "current_value": "1"},
}


def create_database(path="./databases", name="tmp"):
    file_path = os.path.join(path, name)

    if os.path.exists(file_path):
        return "Database already exists"
    else:
        try:
            os.mkdir(file_path)
        except:
            return "Couldn't create database. Please retry."
    return "Database successfully created"


def check_database_exists(path="./databases", name="tmp"):
    file_path = os.path.join(path, name)
    return os.path.exists(file_path)


def delete_database(path="./databases", name="tmp"):
    file_path = os.path.join(path, name)
    try:
        shutil.rmtree(file_path)
    except:
        return "Couldn't delete database. Make sure it exists first"
    return "Database successfully deleted"


def make_table(path="./databases", database_name="test", table_name="tmp", headers="emptyheaders"):
    file_path = os.path.join(path, database_name, table_name + ".csv")
    try:
        os.stat(file_path)
    except:
        fh = open(file_path, 'w')
        fh.write(headers)
        fh.close()
        return "Table successfully created"
    return "Table already exists"


def check_table_exists(path="./databases", database_name="test", table_name="tmp"):
    file_path = os.path.join(path, database_name, table_name + ".csv")
    return os.path.exists(file_path)


def insert_into_table(database, table, columns,  values):
    ordered_values = []
    mapping = {}
    file_path = os.path.join("./databases", database, table + ".csv")

    try:
        fr = open(file_path, 'r')
    except:
        return "Table could not be locked for writing"

    if len(columns) != len(values):
        return "There is a syntax error in your query."

    headers = fr.readline().strip('\n').split(',')
    columns_type = [header.split()[1].strip('()') for header in headers]
    return_headers = [header.split()[0] for header in headers]

    fr.close()
    fw = open(file_path, 'a')
    line_to_write = "%s,"*(len(return_headers)-1) + "%s"

    for i, val in enumerate(columns):
        mapping[columns[i]] = str(values[i])
    for col in columns:
        if col not in return_headers:
            fw.close()
            return "Column %s doesn't exist in the table" % col

    for header in return_headers:
        try:
            # here should be a type checking(don't try to insert strings to int types)
            ordered_values.append(mapping[header])
        except:
            ordered_values.append('NULL')

    fw.write("\n" + line_to_write % tuple(ordered_values))
    fw.close()
    return "Values successfully inserted"


def select_from_table(database, table, columns_to_select, condition=[], operation=[], values=[], in_between=[], include_null=False):
    result = []
    precomputed_result = []
    file_path = os.path.join("./databases", database, table + ".csv")
    try:
        fr = open(file_path, 'r')
    except:
        fr.close()
        return "Table could not be locked for reading"

    headers = fr.readline().strip('\n').split(',')
    columns_type = [header.split()[1].strip('()') for header in headers]
    return_headers = [header.split()[0] for header in headers]
    if columns_to_select != ["*"]:
        for col in columns_to_select:
            if col not in return_headers:
                fr.close()
                print("Column(s) %s doesn't exist in the table" % col)
                return
    Lines = fr.readlines()
    if condition != []:
        columns_to_compare_indexes = []
        for cond in condition:
            columns_to_compare_indexes.append(return_headers.index(cond))

    if columns_to_select != ["*"]:
        set_difference = set(return_headers) - set(columns_to_select)

        list_difference = list(set_difference)
        diff_indexes = []
        for val in list_difference:
            diff_indexes.append(return_headers.index(val))
    for line in Lines:
        precomputed_result = line.strip().split(',')
        if condition != []:
            if not line_met_condition(precomputed_result, operation, values, columns_to_compare_indexes, columns_type, in_between, include_null):
                continue
        if columns_to_select != ["*"]:
            for ind in sorted(diff_indexes, reverse=True):
                precomputed_result.pop(ind)

        result.append(precomputed_result)
    fr.close()
    if columns_to_select != ["*"]:
        return columns_to_select, result, columns_type
    else:
        return return_headers, result, columns_type


def delete_from_table(database, table, condition=[], operation=[], values=[], in_between=[]):
    count_delete = 0
    precomputed_result = []
    print(database, table, condition, operation, values, in_between)
    file_path = os.path.join("./databases", database, table + ".csv")
    try:
        fr = open(file_path, 'r')
    except:
        return "Table could not be locked for reading"

    headers = fr.readline().strip('\n').split(',')
    columns_type = [header.split()[1].strip('()') for header in headers]
    return_headers = [header.split()[0] for header in headers]
    Lines = fr.readlines()
    fr.close()
    try:
        fw = open(file_path, 'w')
    except:
        return "Table could not be locked for deleting"
    if condition != []:
        columns_to_compare_indexes = []
        for cond in condition:
            columns_to_compare_indexes.append(return_headers.index(cond))
    else:
        fw.write(','.join(headers))
        fw.close()
        return "Found and deleted %s line(s)" % str(len(Lines))
    fw.write(','.join(headers))
    for line in Lines:
        precomputed_result = line.strip().split(',')
        if condition != []:
            if not line_met_condition(precomputed_result, operation, values, columns_to_compare_indexes, columns_type, in_between):
                fw.write("\n" + line.strip('\n'))
            else:
                count_delete += 1

    fw.close()
    return "Found and deleted %s lines" % count_delete


def update_table(database, table, columns_to_set, values_to_set, condition=[], operation=[], values=[], in_between=[]):
    count_update = 0
    precomputed_result = []
    file_path = os.path.join("./databases", database, table + ".csv")
    try:
        fr = open(file_path, 'r')
    except:
        return "Table could not be locked for reading"

    headers = fr.readline().strip('\n').split(',')
    columns_type = [header.split()[1].strip('()') for header in headers]
    return_headers = [header.split()[0] for header in headers]
    Lines = fr.readlines()
    fr.close()
    try:
        fw = open(file_path, 'w')
    except:
        return "Table could not be locked for deleting"

    columns_to_set_indexes = []
    for colts in columns_to_set:
        columns_to_set_indexes.append(return_headers.index(colts))
    if condition != []:
        columns_to_compare_indexes = []
        for cond in condition:
            columns_to_compare_indexes.append(return_headers.index(cond))
    fw.write(','.join(headers))
    for line in Lines:
        precomputed_result = line.strip().split(',')
        if condition != []:
            if not line_met_condition(precomputed_result, operation, values, columns_to_compare_indexes, columns_type, in_between):
                fw.write("\n" + line.strip('\n'))
            else:
                seq = 0
                for ind in columns_to_set_indexes:
                    precomputed_result[ind] = str(values_to_set[seq])
                    seq += 1
                fw.write("\n" + ','.join(precomputed_result))
                count_update += 1
        else:
            seq = 0
            for ind in columns_to_set_indexes:
                precomputed_result[ind] = str(values_to_set[seq])
                seq += 1
            fw.write("\n" + ','.join(precomputed_result))
            count_update += 1
    fw.close()
    return "Found and updated %s line(s)" % count_update


def line_met_condition(line=[], operation=[], values=[], columns_to_compare_indexes=[], columns_type=[], in_between=[], include_null=False):
    if columns_to_compare_indexes == []:
        return True

    eval_condition = "%s "
    for condition in in_between:
        eval_condition += condition+" %s "

    for i, val in enumerate(columns_to_compare_indexes):
        # always return NULL values as we need to impute them later
        # after the impute, if the value doesn't match the condition will be removed
        line_ok = True
        if line[columns_to_compare_indexes[i]] == 'NULL':
            line_ok = include_null
        if columns_type[i] == 'string' or columns_type[i] == 'bool':
            right_operand = str(values[i])
            left_operand = str(line[columns_to_compare_indexes[i]])
        else:
            if line[columns_to_compare_indexes[i]] != 'NULL':
                right_operand = int(float(values[i]))
                left_operand = int(float(line[columns_to_compare_indexes[i]]))
            else:
                return include_null
        if operation[i] == "<":
            if left_operand >= right_operand:
                line_ok = False
        if operation[i] == ">":
            if left_operand <= right_operand:
                line_ok = False
        if operation[i] == "=":
            if left_operand != right_operand:
                line_ok = False
        if operation[i] == "<=":
            if left_operand > right_operand:
                line_ok = False
        if operation[i] == ">=":
            if left_operand < right_operand:
                line_ok = False
        if operation[i] == "!=":
            if left_operand == right_operand:
                line_ok = False
        eval_condition = eval_condition.replace("%s", str(line_ok), 1)
    return eval(eval_condition)


def pretty_print(headers, results, percentage=[[]]):
    t = Texttable()
    if percentage != [[]]:
        t.add_rows([headers]+results+[percentage])
    else:
        t.add_rows([headers]+results)
    print(t.draw())


def parse_command(command):
    new_command = re.split('([^a-zA-Z0-9><!=_.])', ''.join(command))
    new_command = list(filter(lambda a: a != "", new_command))
    new_command = list(filter(lambda a: a != ",", new_command))
    new_command = list(filter(lambda a: a != " ", new_command))
    return new_command


def make_imputation(data, values_to_impute_with):
    # do not modify the content from data
    # copy the content to temp list
    local_data = data[:]
    for i, val in enumerate(data):
        local_data[i] = data[i][:]

    for i, val in enumerate(local_data[0]):
        for j, val in enumerate(local_data):
            if local_data[j][i] == 'NULL':
                local_data[j][i] = values_to_impute_with[i]
    return local_data


def count_null_percentage(data):
    row_length = len(data[0])
    no_of_rows = len(data)
    percentage = [0]*row_length
    for i, val in enumerate(data[0]):
        for j in range(no_of_rows):
            if data[j][i] == 'NULL' or data[j][i]==np.NaN:

                percentage[i] += 1
    return [str(round(value/no_of_rows*100, 2)) for value in percentage]
