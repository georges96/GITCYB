
import os
import sys
import time

import database_utils
import impute_methods

database = ""
command = ""
global_variables = database_utils.global_variables
methods_to_impute_with = {
"average": "impute_by_average",
"frequent": "impute_by_most_frequent"
}
#getattr(impute_methods, methods_to_impute_with["frequent"])([[1,2,3],[1,3,4]])
while command != 'exit' and command!='EXIT':
	command = input("> ")
	#command = command.upper()
	to_execute = database_utils.parse_command(command)
	#works with create database some_name
	if to_execute[0].upper() == 'CREATE' and to_execute[1].upper() == 'DATABASE':
		if len(to_execute) == 3:
			print( "> " + database_utils.create_database("./databases", to_execute[2]))
		else:
			print ("> There is a syntax error in your query")

	#works with: use some_database_name
	if to_execute[0].upper() == 'USE':
		if len(to_execute) == 2:
			if database_utils.check_database_exists("./databases", to_execute[1]):
				database = to_execute[1]
				print ("> The selected database is now: %s"%database)
			else:
				print ("> The database you are trying to use doesn't exist")
		else:
			print ("> There is a syntax error in your query")
	#works with: SET impute_method = average
	if to_execute[0].upper() == 'SET':
		if len(to_execute) == 4:
			if to_execute[1] in global_variables.keys():
				if to_execute[2] == "=":
					if "allowed_values" in global_variables[to_execute[1]].keys():
						if to_execute[3] in global_variables[to_execute[1]]["allowed_values"]:
							global_variables[to_execute[1]]["current_value"] = to_execute[3]
							print ("> Variable %s successfully updated"%to_execute[1])
						else:
							print ("> The allowed values are %s "%(global_variables[to_execute[1]]["allowed_values"]))
			else:
				print ("> There is no such global variable")
		else:
			print ("> There is a syntax error in your query")

	#works with: delete database some_database_name
	if to_execute[0].upper() == 'DELETE' and to_execute[1].upper() == 'DATABASE':
		if len(to_execute) == 3:
			database = ""
			print (database_utils.delete_database("./databases", to_execute[2]))
		else:
			print ("> There is a syntax error in your query")

	#works with: create table tablename col1 (int), col2 (bool), col3 (string)
	if to_execute[0].upper() == 'CREATE' and to_execute[1].upper() == 'TABLE':
		if database != "":
			headers = []
			ok = 1 
			for i in range(3, len(to_execute)-3, 4):
				if (len(to_execute)-3)%4 == 0:
					if to_execute[i] and to_execute[i+1] == '(' and to_execute[i+2] in ('string', 'int', 'bool') and to_execute[i+3] == ')':
						headers.append(" ".join((to_execute[i], "".join(to_execute[i+1:i+4]))))
					else:
						ok = 0
				else:
					ok = 0
			if not ok:
				print("> There is an error in your syntax. Define the column as column_name(type) where type is on of 'string', 'bool', 'int'")
			else:
				headers=",".join(headers)
				print( "> " + database_utils.make_table("./databases", database, to_execute[2], headers))
		else:
			print ("> You haven't selected any database")

	#works with: SELECT * FROM table_name 
	#works with: SELECT col1,col2 FROM table_name 
	#works with: SELECT * FROM table_name WHERE col1 > some_value, col2 < some_value
	#for now it's only applying AND
	if to_execute[0].upper() == 'SELECT':
		columns_to_select = []
		columns_to_compare = []
		operations = []
		values_to_compare = []
		where_clause = []
		try:
			columns_to_select = to_execute[to_execute.index('SELECT')+1:to_execute.index('FROM')]
			table = to_execute[to_execute.index('FROM')+1]
			if 'WHERE' in to_execute:
				where_clause = to_execute[to_execute.index('WHERE')+1:]
			for i in range(0, len(where_clause), 3):
				columns_to_compare.append(where_clause[i])
				operations.append(where_clause[i+1])
				values_to_compare.append(where_clause[i+2])
			print (columns_to_compare, operations, values_to_compare)
			if database != "":
				
				if database_utils.check_table_exists(database_name=database, table_name=table):
					start_time = time.time()
					headers,results = database_utils.select_from_table(database, table, columns_to_select, columns_to_compare, operations, values_to_compare)
					if results:
						if global_variables['impute_method']['current_value'] != "no":
							values_to_impute_with = getattr(impute_methods, methods_to_impute_with[global_variables['impute_method']['current_value']])(results)
							imputed_results = database_utils.make_imputation(results, values_to_impute_with)
							database_utils.pretty_print(headers,imputed_results)
						else:
							database_utils.pretty_print(headers, results)
					end_time = time.time()
					print("Found and returned %s line(s)"%len(results))
					print("Query executed in %s seconds"%(end_time-start_time))
				else:
					print ("> The table doesn't exist")
			else:
				print ("> You haven't selected any database")
		except Exception as e:
			print ("> There is an error in your syntax.", e)
	
	#works with: INSERT INTO table_name col1,col2 VALUES 1,2
	#if column is not specified it will add a NULL
	if to_execute[0].upper() == 'INSERT' and to_execute[1].upper() == 'INTO':
		if 'INTO' in to_execute:
			table = to_execute[to_execute.index('INTO')+1]
		if 'VALUES' in to_execute:
			columns_to_insert = to_execute[to_execute.index('INTO')+2:to_execute.index('VALUES')]
			values_to_insert = to_execute[to_execute.index('VALUES')+1:]
		else:
			pass
		if database != "":
			start_time = time.time()
			print(database_utils.insert_into_table(database, table, columns_to_insert, values_to_insert))
			end_time = time.time()
			print("Query executed in %s seconds"%(end_time-start_time))
		else:
			print ("> You haven't selected any database")

	#works with: DELETE FROM table_name
	#works with: DELETE FROM table_name WHERE col1 < 44
	if to_execute[0].upper() == 'DELETE' and to_execute[1].upper() == 'FROM':
		columns_to_compare = []
		operations = []
		values_to_compare = []
		where_clause = []
		table = to_execute[to_execute.index('FROM')+1]
		if 'WHERE' in to_execute:
			where_clause = to_execute[to_execute.index('WHERE')+1:]
		for i in range(0, len(where_clause), 3):
			columns_to_compare.append(where_clause[i])
			operations.append(where_clause[i+1])
			values_to_compare.append(where_clause[i+2])
		if database != "":
			if database_utils.check_table_exists(database_name=database, table_name=table):
				start_time = time.time()
				print(database_utils.delete_from_table(database, table, columns_to_compare, operations, values_to_compare))
				end_time = time.time()
				print("Query executed in %s seconds"%(end_time-start_time))
			else:
				print ("> The table doesn't exist")
		else:
			print ("> You haven't selected any database")

	#works with: UPDATE table_name SET col1 = 45 WHERE col1 > 3
	#works with: UPDATE table_name SET col1 = 45
	if to_execute[0].upper() == 'UPDATE' and to_execute[2].upper() == 'SET':
		columns_to_compare = []
		operations = []
		values_to_compare = []
		where_clause = []
		
		set_columns_values = []
		columns_to_update = []
		values_to_update = []

		table = to_execute[to_execute.index('UPDATE')+1]

		if 'WHERE' in to_execute:
			where_clause = to_execute[to_execute.index('WHERE')+1:]
			set_columns_values = to_execute[to_execute.index('SET')+1:to_execute.index('WHERE')]
		else:
			set_columns_values = to_execute[to_execute.index('SET')+1:]
		print (set_columns_values)
		for i in range (0, len(set_columns_values),3):
			columns_to_update.append(set_columns_values[i])
			values_to_update.append(set_columns_values[i+2])
		for i in range(0, len(where_clause), 3):
			columns_to_compare.append(where_clause[i])
			operations.append(where_clause[i+1])
			values_to_compare.append(where_clause[i+2])
		if database != "":
			if database_utils.check_table_exists(database_name=database, table_name=to_execute[1]):
				start_time = time.time()
				print(database_utils.update_table(database, table, columns_to_update, values_to_update, columns_to_compare, operations, values_to_compare))
				end_time = time.time()
				print("Query executed in %s seconds"%(end_time-start_time))
			else:
				print ("> The table doesn't exist")
		else:
			print ("> You haven't selected any database")

