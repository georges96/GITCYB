import sys
import os

import database_utils

database = ""
command = ""

while command != 'exit':
	command = input("> ")

	to_execute = command.split()#this should be a parse_command function
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

	#works with: delete database some_database_name
	if to_execute[0].upper() == 'DELETE' and to_execute[1].upper() == 'DATABASE':
		if len(to_execute) == 3:
			database = ""
			print (database_utils.delete_database("./databases", to_execute[2]))
		else:
			print ("> There is a syntax error in your query")

	#works with: create table table_name col1,col2,col3
	if to_execute[0].upper() == 'CREATE' and to_execute[1].upper() == 'TABLE':
		if database != "":
			print( "> " + database_utils.make_table("./databases", database, to_execute[2], to_execute[3]))
		else:
			print ("> You haven't selected any database")

	#works with: select * from table_name 
	#works with: select some_column from table_name 
	if to_execute[0].upper() == 'SELECT' and to_execute[2].upper() == 'FROM':
		if database != "":
			if database_utils.check_table_exists(database_name=database, table_name=to_execute[3]):
				database_utils.select_from_table(database, to_execute[3], to_execute[1])
			else:
				print ("> The table doesn't exist")
		else:
			print ("> You haven't selected any database")
	
	#works with: insert into table_name col1,col2 1,2
	#if column is not specified it will add a NULL
	if to_execute[0].upper() == 'INSERT' and to_execute[1].upper() == 'INTO':
		if database != "":
			print(database_utils.insert_into_table(database, to_execute[2], [to_execute[3]], [to_execute[4]]))
		else:
			print ("> You haven't selected any database")

	#works with: delete from table_name
	if to_execute[0].upper() == 'DELETE' and to_execute[1].upper() == 'FROM':
		if database != "":
			if database_utils.check_table_exists(database_name=database, table_name=to_execute[2]):
				#print(database_utils.delete_from_table(database, to_execute[2], ['col1'], ['>'],[5]))
				print(database_utils.delete_from_table(database, to_execute[2]))
			else:
				print ("> The table doesn't exist")
		else:
			print ("> You haven't selected any database")

	#works with: update table_name set col1 45
	if to_execute[0].upper() == 'UPDATE' and to_execute[2].upper() == 'SET':
		if database != "":
			if database_utils.check_table_exists(database_name=database, table_name=to_execute[1]):
				#print(database_utils.update_table(database, to_execute[1], ['col1'], [32], ['col2', 'col3'], ['>', '='], [4, 3]))
				print(database_utils.update_table(database, to_execute[1], [to_execute[3]], [to_execute[4]]))
			else:
				print ("> The table doesn't exist")
		else:
			print ("> You haven't selected any database")

