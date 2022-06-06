import requests
import time
import sys, getopt
from BD.create_database import create_conection
from BD.create_database import load_database_strucure

totalPokemos = 50
url_api = 'https://pokeapi.co/api/v2/'

def populate_pokemon():
	print("\nPopulating Pokemon List")
	endpoint = url_api + 'pokemon?limit=50'
	while(endpoint != None):
		response = requests.get(endpoint);
		body = response.json()

		endpoint = body['next']
		offset = getOffset(endpoint)

		lastPage = isinstance(offset, int) and offset >= totalPokemos
		processing = offset if not lastPage else 'Last Page'
		print('processing: {}/{}'.format(processing, totalPokemos))

		insert_pokemons(body['results'])
		if lastPage:
			endpoint = None

def getOffset(url):
	if not url:
		return None

	index1 = url.index('=') + 1
	index2 = url.index('&')
	offset = url[index1: index2]
	return int(offset)

def insert_pokemons(results):
	for result in results:
		name = result['name']
		url = url_api+'pokemon'+'/{}'.format(name)
		response = requests.get(url);
		body = response.json()
		insert_pokemon(body['name'], body['id'], result['url'], body['height'])

		for ability in body['abilities']:
			insert_pokemon_ability(body['name'], ability['ability']['name'])

		for type in body['types']:
			insert_pokemon_type(body['name'], type['type']['name'])

def insert_pokemon(name, idApip, url, height):
	insert_sql = 'INSERT into public.pokemon(id_api, name, height, url) values(%s, %s, %s, %s)'
	con = create_conection()
	with con.cursor() as cursor:
		execute_sql(con, cursor, insert_sql, (idApip,name,height,url))
		con.close()

def insert_pokemon_ability(namePokemon, nameAbility):
	insert_sql_ability = '''INSERT into public.pokemon_abiliy(id_pokemon, id_ability)
		values((SELECT pp.id FROM public.pokemon pp WHERE pp.name = %s), (SELECT pa.id FROM public.ability pa WHERE pa.name = %s)) 
		'''
	values = (namePokemon, nameAbility)
	con = create_conection()
	with con.cursor() as cursor:
		execute_sql(con, cursor, insert_sql_ability, values)

def insert_pokemon_type(namePokemon, nameType):
	insert_sql_type = '''INSERT into public.pokemon_type(id_pokemon, id_type)
		values((SELECT pp.id FROM public.pokemon pp WHERE pp.name = %s),(SELECT pt.id FROM public.type pt WHERE pt.name = %s)) 
		'''
	values = (namePokemon, nameType)
	con = create_conection()
	with con.cursor() as cursor:
		execute_sql(con, cursor, insert_sql_type, values)

def populate_type():
	print("\nPopulating Type List")
	damage_relations = []
	endpoint = url_api + 'type?limit=10'

	while(endpoint != None):
		response = requests.get(endpoint);
		body = response.json()

		endpoint = body['next']
		offset = getOffset(endpoint)

		processing = offset if body['next'] != None else 'Last Page'
		print('processing: {}/{}'.format(processing, body['count']))

		con = create_conection()
		for type in body['results']:
			responseType = requests.get(type['url']);
			bodyType = responseType.json()
			insert_sql = 'INSERT into public.type(id_api, name, url) values(%s, %s, %s)'
			
			damage_relations.append({'name': bodyType['name'], 'relations': bodyType['damage_relations']})
			with con.cursor() as cursor:
				execute_sql(con, cursor, insert_sql, (bodyType['id'], type['name'], type['url']))
		
		con.close()
	populate_damage_relations(damage_relations);

def populate_damage_relations(types):
	con = create_conection()
	for type in types:
		relationsType = type['relations']

		# para cada tipo de relacao
		for relationType in relationsType.keys():
			relations = relationsType[relationType]

			for relation in relations:
				name = relation['name']
				ty = next((t for t in types if t['name'] == name), None)
				if ty:
					insert_sql = '''INSERT into public.damage_relations(id_type_receiver, id_type_issuer, type_relation)
					values(
						(SELECT pt.id FROM public.type pt WHERE pt.name = %s),
						(SELECT pt1.id FROM public.type pt1 WHERE pt1.name = %s),
						%s
					) 
					'''
					values = (type['name'], ty['name'], relationType)
					with con.cursor() as cursor:
						execute_sql(con, cursor, insert_sql, values)
				
	return

def populate_ability():
	print("\nPopulating Ability List")
	endpoint = url_api + 'ability?limit=50'
	while(endpoint != None):
		response = requests.get(endpoint);
		body = response.json()
		endpoint = body['next']
		offset = getOffset(endpoint)

		processing = offset if body['next'] != None else 'Last Page'
		print('processing: {}/{}'.format(processing, body['count']))

		con = create_conection()
		for ability in body['results']:
			responseAbility = requests.get(ability['url']);
			bodyAbility = responseAbility.json()

			insert_sql = 'INSERT into public.ability(id_api, name, url, effect, short_effect) values(%s, %s, %s, %s, %s)'
			
			with con.cursor() as cursor:
				effectObject = getEffect(bodyAbility['effect_entries'])
				effect = str(effectObject['effect']) if 'effect' in effectObject else None
				shortEffect = str(effectObject['short_effect']) if 'short_effect' in effectObject else None

				tuple = (str(bodyAbility['id']), ability['name'], ability['url'], effect, shortEffect)
				execute_sql(con, cursor, insert_sql, tuple)
		
		con.close()

def getEffect(effectEntries):
	ternary = (effectEntries[0] if (not effectEntries) & len(effectEntries) > 0 else {})
	return next((a for a in effectEntries if a['language']['name'] == 'en'), ternary)


def execute_sql(con, cursor, sql, tuple):
	cursor.execute(sql, tuple)
	con.commit()

def populate_tables():
	t = time.time()
	load_database_strucure()
	populate_ability()
	populate_type()
	populate_pokemon()

	elapsed = time.time() - t
	print('Tempo gasto no processo (segundos): {}'.format(elapsed))


if __name__ == '__main__':
	opts = []
	argv = sys.argv[1:]
	try:
		opts, args = getopt.getopt(argv,"t:",['tpokemon='])
	except getopt.GetoptError as error:
		print(error)

	for opt, arg in opts:
		if opt in ("-t", "--tpokemon"):
			totalPokemos = int(arg)

	populate_tables()