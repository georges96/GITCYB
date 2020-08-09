#Takes a path to the csv file, the number of values to nullify
#for each row and the columns to ignore from nullifying
import random
import click

def random_nullifier(path_to_csv, number_of_values=1, ignore_columns=[]):
	
	try:
		fr = open(path_to_csv, 'r')
	except:
		fr.close()
		print("Read operation cannot be performed")

	#first_line = fr.readline()
	headers = fr.readline().strip('\n').split(',')
	no_columns_in_file = len(headers)
	if number_of_values > no_columns_in_file:
		print("You cannot ask to nullify more columns than available")
		return
	seqeuence_to_choose_from = [x for x in range(no_columns_in_file)]
	seqeuence_to_choose_from = list(set(seqeuence_to_choose_from) - set(ignore_columns))
	if seqeuence_to_choose_from == []:
		print("You want to ignore all of the columns from nullifying. Nothing to do")
		return
	if number_of_values > no_columns_in_file - len(ignore_columns):
		number_of_values -= len(ignore_columns)
		print("Adjusted the number of values to %s based on ignore_columns" % number_of_values)

	try:
		fw = open(path_to_csv+'_nulled.csv', 'w')
	except:
		fw.close()
		print("Write operation cannot be performed")
	fw.write(','.join(headers))
	Lines = fr.readlines()
	for line in Lines:
		no_v = number_of_values
		precomputed_result = line.strip().split(',')
		while no_v > 0:
			index_to_be_nullified = random.choice(seqeuence_to_choose_from)
			if precomputed_result[index_to_be_nullified]!='NULL':
				no_v -= 1
			precomputed_result[index_to_be_nullified] = 'NULL'
		fw.write("\n" + ','.join(precomputed_result))
	

@click.command()
@click.option('-p', required=True, help='Path to the csv file to be nullified')
@click.option('-n', default=1, help='How many columns in each row to nullify')
@click.option('-i', default=[], help='A list of columns to ignore from nullifying')
def cli(p, n, i):
	if i!=[]:
		i = [int(x) for x in i.split(',')]
	random_nullifier(p, n, i)
if __name__ == '__main__':
    cli()
    