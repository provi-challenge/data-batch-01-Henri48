import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
	con = psycopg2.connect(host='localhost',
		port=5444,
		user='postgres', 
		password='postgres')

	# Create DataBase if not 
	con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
	with con.cursor() as cursor:
		cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'data_challange'")
		exists = cursor.fetchone()
		if not exists:
				cursor.execute('CREATE DATABASE data_challange')

def create_conection(): 
	con = psycopg2.connect(host='localhost', 
		port='5444',
		database='data_challange',
		user='postgres', 
		password='postgres')
	return con

def query_db(sql):
  con = create_conection()
  cur = con.cursor()
  cur.execute(sql)
  recset = cur.fetchall()
  registros = []
  for rec in recset:
    registros.append(rec)
  con.close()
  return registros

def create_table_sql(tableName, props_table):
	sql = 'CREATE TABLE public.{tableName} ( \n'.format(tableName=tableName)

	for prop in props_table:
		sql_column = '{name} {type} {null_able}, '.format(name=prop['name'], type=prop['type'], null_able=prop['null_able'])
		sql += sql_column

	sql = sql[:-2]
	sql += ')'
	return sql

def create_table(tableName, props_table):

	sql = 'DROP TABLE IF EXISTS public.{tableName} CASCADE'.format(tableName=tableName)
	sql_table = create_table_sql(tableName, props_table)

	try:
		con = create_conection()
		cur = con.cursor()
		cur.execute(sql)
		cur.execute(sql_table)
		con.commit()
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		print('\n'+"Error: %s" % error)
		con.rollback()
		cur.close()
		raise Exception(error)

def add_unique_constraint(tableName, columns):
  
	listColumns = []
	for col in columns:
		listColumns.append(col)

	sql = 'ALTER TABLE public.{tableName} ADD UNIQUE ({columnList})'.format(tableName=tableName, columnList=', '.join(listColumns))
	try:
		con = create_conection()
		cur = con.cursor()
		cur.execute(sql)
		con.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print('\n'+"Error: %s" % error)
		con.rollback()
		cur.close()
		raise Exception(error)
	cur.close()

def create_foreignkey(tableName, constraintName, columnName, tableReferenceName, columnReferenceName):
	check_foreignkey_exist = 'ALTER TABLE public.{tableName} DROP CONSTRAINT IF EXISTS {constraintName}'.format(
		tableName=tableName,
		constraintName=constraintName)

	constraint_template = 'ALTER TABLE public.{tableName} ADD CONSTRAINT {constraintName} FOREIGN KEY ({columnName}) REFERENCES {tableReferenceName}({columnReferenceName}) ON DELETE CASCADE'
	constraint_sql = constraint_template.format(
		tableName=tableName,
		constraintName=constraintName,
		columnName=columnName,
		tableReferenceName=tableReferenceName,
		columnReferenceName=columnReferenceName)

	try:
		con = create_conection()
		cur = con.cursor()
		cur.execute(check_foreignkey_exist)
		cur.execute(constraint_sql)
		con.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print('\n'+"Error: %s" % error)
		con.rollback()
		cur.close()
		return 1
	cur.close()

def load_database_strucure():
	print('Creating database...')
	create_database()

	props = [
		{'name': 'id',			'type': 'SERIAL PRIMARY KEY', 	'null_able': 'NOT NULL'},
		{'name': 'id_api', 		'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'data_ins',	'type': 'TIMESTAMP', 			'null_able': 'NOT NULL DEFAULT CURRENT_TIMESTAMP'},
		{'name': 'name',		'type': 'VARCHAR(50)',			'null_able': 'NOT NULL'},
		{'name': 'height', 		'type': 'SMALLINT', 			'null_able': 'NOT NULL'},
		{'name': 'url', 		'type': 'VARCHAR(150)',			'null_able': 'NOT NULL'}
	]
	create_table('pokemon', props)
	
	props = [
		{'name': 'id',			'type': 'SERIAL PRIMARY KEY', 	'null_able': 'NOT NULL'},
		{'name': 'id_api', 		'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'name',		'type': 'VARCHAR(50)',			'null_able': 'NOT NULL'},
		{'name': 'data_ins',	'type': 'TIMESTAMP', 			'null_able': 'NOT NULL DEFAULT CURRENT_TIMESTAMP'},
		{'name': 'url', 		'type': 'VARCHAR(150)',			'null_able': 'NOT NULL'},
		{'name': 'effect', 		'type': 'TEXT', 				'null_able': 'NULL'},
		{'name': 'short_effect','type': 'TEXT', 				'null_able': 'NULL'},
	]
	create_table('ability', props)
	
	props = [
		{'name': 'id',			'type': 'SERIAL PRIMARY KEY', 	'null_able': 'NOT NULL'},
		{'name': 'id_api', 		'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'data_ins',	'type': 'TIMESTAMP', 			'null_able': 'NOT NULL DEFAULT CURRENT_TIMESTAMP'},
		{'name': 'name',		'type': 'VARCHAR(50)',			'null_able': 'NOT NULL'},
		{'name': 'url', 		'type': 'VARCHAR(150)',			'null_able': 'NOT NULL'}
	]
	create_table('type', props)
	
	props = [
		{'name': 'id',					'type': 'SERIAL PRIMARY KEY',	'null_able': 'NOT NULL'},
		{'name': 'id_type_receiver',	'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'id_type_issuer',		'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'type_relation',		'type': 'VARCHAR(50)',			'null_able': 'NOT NULL'},
		{'name': 'data_ins',			'type': 'TIMESTAMP', 			'null_able': 'NOT NULL DEFAULT CURRENT_TIMESTAMP'},
	]
	create_table('damage_relations', props)
	
	props = [
		{'name': 'id',			'type': 'SERIAL PRIMARY KEY',	'null_able': 'NOT NULL'},
		{'name': 'id_pokemon',	'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'id_type',		'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'data_ins',	'type': 'TIMESTAMP', 			'null_able': 'NOT NULL DEFAULT CURRENT_TIMESTAMP'},
	]
	create_table('pokemon_type', props)
	
	props = [
		{'name': 'id',			'type': 'SERIAL PRIMARY KEY',	'null_able': 'NOT NULL'},
		{'name': 'id_pokemon',	'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'id_ability',	'type': 'INT', 					'null_able': 'NOT NULL'},
		{'name': 'data_ins',	'type': 'TIMESTAMP', 			'null_able': 'NOT NULL DEFAULT CURRENT_TIMESTAMP'},
	]
	create_table('pokemon_abiliy', props)

	create_foreignkey('damage_relations', 'fk_id_type_receiver', 'id_type_receiver', 'type', 'id')
	create_foreignkey('damage_relations', 'fk_id_type_issuer', 'id_type_issuer', 'type', 'id')

	create_foreignkey('pokemon_type', 'fk_id_pokemon', 'id_pokemon', 'pokemon', 'id')
	create_foreignkey('pokemon_type', 'fk_id_type', 'id_type', 'type', 'id')

	create_foreignkey('pokemon_abiliy', 'fk_id_pokemon', 'id_pokemon', 'pokemon', 'id')
	create_foreignkey('pokemon_abiliy', 'fk_id_ability', 'id_ability', 'ability', 'id')

	add_unique_constraint('pokemon_type', ['id_pokemon', 'id_type'])
	add_unique_constraint('pokemon_abiliy', ['id_pokemon', 'id_ability'])
