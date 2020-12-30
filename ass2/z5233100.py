import pandas as pd
from flask import Flask
from flask_restplus import Resource, Api, reqparse
import json
import sqlite3
from datetime import datetime
import requests


app = Flask(__name__)
api = Api(app,
		  default="World Bank",
		  title="World Bank Database")

q1_parser = reqparse.RequestParser()
q1_parser.add_argument('indicator_id')

q3_parser = reqparse.RequestParser()
q3_parser.add_argument('order_by')

q6_parser = reqparse.RequestParser()
q6_parser.add_argument('q')


def get_URL(indicator):
	return f'http://api.worldbank.org/v2/countries/all/indicators/{indicator}?'\
		   f'date=2012:2017&format=json&per_page=1000'

def create_db(file):
	con = sqlite3.connect(db)
	cur = con.cursor()
	# create table
	cur.execute('''CREATE TABLE IF NOT EXISTS record
				(id integer primary key AUTOINCREMENT, 
				indicator text not NULL, 
				indicator_value text,
				creation_time text,
				entries text)''')
	# save
	con.commit()
	con.close()

def response_format(uri, id, creation_time, indicator_id, question='q3'):
	if question == 'q3':
		return {
				"uri": uri,
				"id": id,
				"creation_time": creation_time,
				"indicator": indicator_id
				}
	elif question == 'q1':
		return {
				"uri": uri,
				"id": id,
				"creation_time": creation_time,
				"indicator_id": indicator_id
				}

def dataframe_to_list(df):
	l = []
	for index, row in df.iterrows():
		row_list = response_format(row['uri'],row['id'],
					row['creation_time'],row['indicator'])
					
		l.append(row_list)
	return l

@api.route('/collections')
class CollectionsImport(Resource):
	#q3
	@api.response(200, 'Successful')
	@api.response(400, 'Validation Error')
	# @api.doc(description="Get all collections with a specific order")
	@api.expect(q3_parser)
	def get(self):

		args = q3_parser.parse_args()
		order = args['order_by']

		# retrieve from database
		con = sqlite3.connect(db)
		cur = con.cursor()
		cur.execute('SELECT * FROM record')
		retrieval = cur.fetchall()
		con.close()

		result = []
		for ret in retrieval:
			col_uri = f"/collections/{ret[0]}"
			ret_dict = response_format(col_uri, ret[0], ret[3], ret[1])
			result.append(ret_dict)

		if order:
			features = ['id', 'creation_time', 'indicator']
			order = order.split(',')
			sort_columns = []
			sort_order = []
			# convert to df
			df = pd.DataFrame(data=result)

			for i in order:
				# consider without '+' and '-'
				if i[0] != '-' and i[0] != '+' and i in features:
					sort_columns.append(i)
					sort_order.append(True)

				elif i[1:] in features:
					sort_columns.append(i[1:])
					if i[0] == '-':
						sort_order.append(False)
					elif i[0] == '+':
						sort_order.append(True)
					else:
						return {"message": "Invalid Input, Please try again!"}, 400
				else:
					return {"message": "Invalid Input, Please try again!"}, 400
			df.sort_values(by=sort_columns, ascending=sort_order, inplace=True)
			result = dataframe_to_list(df)

		return result, 200

	#q1
	@api.response(200, 'The collection is already imported')
	@api.response(201, 'Successfully create the collection')
	@api.response(400, 'Validation Error')
	@api.response(404, 'Indicator does not exist')
	@api.expect(q1_parser)
	def post(self):

		information = q1_parser.parse_args()
		indicator = information['indicator_id']

		if not indicator:
			return {"message": "Please input an indicator"}, 400
		elif indicator[-1] == '?' or indicator.isspace():
			return {"message": "Invalid Indicator format"}, 400

		url = get_URL(indicator)
		# fetch the content and convert to json
		json_data = requests.get(url).json()

		# check the indicator
		if len(json_data) <= 1:
			return {"message": "Cannot find Indicator"}, 404

		# construct entry
		entries = []
		for entry in json_data[1]:
			# remove those entries with none value
			if entry['value']:

				entry_dict = {
							'country': entry['country']['value'],
							'date': int(entry['date'][:4]), # the first 4 elements
							'value': entry['value']
							}
				entries.append(entry_dict)

		# indicator information
		indicator_value = json_data[1][0]['indicator']['value']
		creation_time = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
		entries = json.dumps(entries)

		# connect to sql
		con = sqlite3.connect(db)
		cur = con.cursor()
		q = (indicator,)
		cur.execute('SELECT * FROM record WHERE indicator=?', q)
		find = cur.fetchone()
		insert = 0 # record whether the record is inserted

		# the indicator not in record
		if not find:
			cur.execute('''INSERT INTO record (indicator, indicator_value, creation_time, entries) 
				values(?,?,?,?)''', (indicator, indicator_value, creation_time, entries))
			con.commit()
			cur.execute('SELECT * FROM record WHERE indicator=?', q)
			find = cur.fetchone()
			insert = 1
		
		indicator_id = find[0]
		creation_time = find[3]

		con.close()

		col_uri = f"/collections/{indicator_id}"
		collection = response_format(col_uri, indicator_id, creation_time, indicator, 'q1')
		if insert:
			return collection, 201
		else:
			return collection, 200


@api.route('/collections/<int:id>')
class CollectionsId(Resource):

	# q4
	@api.response(404, 'Collection was not found')
	@api.response(200, 'Successful')
	def get(self, id):
		q = (id,)
		con = sqlite3.connect(db)
		cur = con.cursor()
		cur.execute('SELECT * FROM record WHERE id=?', q)
		find = cur.fetchone()
		con.close()
		if find:
			return {
					"id": find[0],
					"indicator": find[1],
					"indicator_value": find[2],
					"creation_time": find[3],
					"entries": json.loads(find[4])
					}, 200
		else:
			return {"message": f"Collection {id} is not in database!"}, 404
	# q2
	@api.response(200, 'Successful')
	@api.response(404, 'Collection was not found')
	def delete(self, id):
		q = (id,)
		con = sqlite3.connect(db)
		cur = con.cursor()
		cur.execute('SELECT * FROM record WHERE id=?', q)
		find = cur.fetchone()
		if find:
			cur.execute('DELETE FROM record WHERE id=?', q)
			con.commit()
			con.close()
			return {
					"message": f"The collection {id} was removed from the database!",
					"id": id
					}, 200
		else:
			con.close()
			return {
					"message": f"Collection {id} is not in database!"
					}, 404


@api.route('/collections/<int:id>/<int:year>/<string:country>')
class CollectionYear(Resource):

	# q5
	@api.response(404, 'Data was not found')
	@api.response(200, 'Successful')
	def get(self, id, year, country):
		q = (id,)
		con = sqlite3.connect(db)
		cur = con.cursor()
		cur.execute('SELECT indicator, entries FROM record WHERE id=?', q)
		find = cur.fetchone()
		con.close()
			
		if not find:
			return {
					"message": f"Collection {id} is not in database!"
					}, 404
		for ent in json.loads(find[1]):
			if ent['date'] == year and ent['country'] == country:
				return {
						"id": id,
						"indicator": find[0],
						"country": country,
						"year": year,
						"value": ent['value']
						}, 200
		# not find country or not find year
		return {
				"message": "No such data!"
				}, 404


@api.route('/collections/<int:id>/<int:year>')
class CollectionRetrieval(Resource):

	@api.response(200, 'Successful')
	@api.response(404, 'Data was not found')
	@api.expect(q6_parser)
	def get(self, id, year):
		q = (id,)
		args = q6_parser.parse_args()
		query = args['q']

		con = sqlite3.connect(db)
		cur = con.cursor()
		cur.execute('SELECT indicator, indicator_value, entries FROM record WHERE id=?', q)
		find = cur.fetchone()
		con.close()

		if not find:
			return {
					"message": f"Collection {id} is not in database!"
					}, 404

		entries_in_year = []
		# collection entries with specific year
		for ent in json.loads(find[2]):
			if ent['date'] == year:
				entry = {
						"country":ent['country'],
						"value": ent['value']
						}
				entries_in_year.append(entry)

		result_entries = entries_in_year

		if len(result_entries) < 1:
			return {
					"message": "No such data!"
			}, 404

		# top or bottom
		if query:
		# check top or bottom
			direction = query[0]
			number = -1 # default value, used to check input

			if direction != '+' and direction != '-' and query.isdigit():
				number = int(query)
				direction = '+'

			elif query[1:].isdigit():
				number = int(query[1:])

			if (direction != '+' and direction != '-') or (number < 1) or (number > 100):
				return {
						"message": "Invalid query!"
						}, 400
			if direction == '+':
				entries_in_year.sort(key=lambda x: x['value'], reverse=True)
			else:
				entries_in_year.sort(key=lambda x: x['value'])

			if number >= len(entries_in_year):
				result_entries = entries_in_year
			else:
				result_entries = entries_in_year[:number]

		return {
				"indicator": find[0],
				"indicator_value": find[1],
				"entries": result_entries
				}, 200


if __name__ == '__main__':
	db = "z5233100.db"
	create_db(db)
	app.run(debug=True)
