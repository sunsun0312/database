'''
COMP9321 2019 Term 1 Assignment Two Code Template
Student Name:GUANQUN ZHOU
Student ID:z5174741
'''

from flask import Flask, request
from flask_restplus import Resource,Api,fields,reqparse
import sqlite3
import requests, datetime


DEBUG = True
MAX_PAGE_LIMIT = 2
COLLECTION = 'indicators'
STORE_collection_id = []
db_file = 'database_assn2.db'
app = Flask(__name__)
app.config.SWAGGER_UI_DOC_EXPANSION = 'list'
api = Api(app)

indicator_model = api.model(COLLECTION, {
  'indicator_id': fields.String(required=True,
                                title='An Indicator ',
                                description='http://api.worldbank.org/v2/indicators',
                                example='NY.GDP.MKTP.CD'),
})

parser = reqparse.RequestParser()
parser.add_argument('query', type=str)

def create_db(db_file):
    conn = sqlite3.connect(db_file)
    try:
        con = conn.cursor()
        con.execute('''CREATE TABLE ASSIGN2
        (indicator text primary key, indicator_value text,creation_time text,collection text,entries text)''')
        conn.commit()
        conn.close()
    except:
        pass
      
def api_url(indicator, date='2013:2018', fmt='json', page=1):
  return 'http://api.worldbank.org/v2/countries/all/indicators/'\
          f'{indicator}?date={date}&format={fmt}&page={page}'

def get_indicator_data(indicator, page=1,prevRes=[], max_pages=2):
  response = requests.get(api_url(indicator=indicator, page=page)).json()
  if not indicator or (len(response) <= 1 and response[0]['message'][0]['key'] == 'Invalid value'):
    return 'Invalid indicator'
  if response[0]['page'] >= max_pages or response[0]['page'] == response[0]['pages']:
    return prevRes+response[1]
  return get_indicator_data(
    indicator=indicator,
    page=response[0]['page']+1,
    prevRes=prevRes+response[1],
    max_pages=max_pages,
  )
def format_entries(indicator_data):
  return {'country': indicator_data['country']['value'],'date': indicator_data['date'],'value': indicator_data['value']}
def merge(left, right):
    result_merge = []
    while left and right:
        if left[0]['value'] <= right[0]['value']:
            result_merge.append(left.pop(0))
        else:
            result_merge.append(right.pop(0))
    if left:
        result_merge += left
    if right:
        result_merge += right
    return result_merge
def merge_sort(L):
    if len(L) <= 1:
        # When D&C to 1 element, just return it
        return L
    mid = len(L) // 2
    left = L[:mid]
    right = L[mid:]
    left = merge_sort(left)
    right = merge_sort(right)
    # conquer sub-problem recursively
    return merge(left, right)
    # return the answer of sub-problem
def sort_entries(entry,year,direct,amount):
    year_sort = str(year)
    store_entry = []
    re_so_entry = []
    for select_year in entry:
        if select_year['date'] == year:
            store_entry.append(select_year)
    for select_value in range(len(store_entry)):
        if store_entry[select_value]['value'] == None:
            store_entry[select_value]['value'] = -1
    so_entry = merge_sort(store_entry)
    if direct == 'top':
        for en_index in range(len(so_entry)-1,len(so_entry)-1-amount,-1):
            re_so_entry.append(so_entry[en_index])
    elif direct == 'bottom':
        for ee_index in range(0,amount):
            re_so_entry.append(so_entry[ee_index])
    for fin in range(len(re_so_entry)):
        if re_so_entry[fin]['value'] == -1:
            re_so_entry[fin]['value'] = None
    return re_so_entry
                    
@api.route(f'/{COLLECTION}', endpoint=COLLECTION)
class CollectionIndex(Resource):
  @api.doc(description='[Q1] Import a collection from the data service.')
  @api.response(200, 'Successfully find the collection.')
  @api.response(201, 'Successfully created collection.')
  @api.response(400, 'Unable to create / retrieve collection.')
  @api.expect(indicator_model)
  def post(self):
    new_request = request.json
    if not new_request['indicator_id']:
      return { 'message': 'Please specify an indicator.' }, 400
    conn = sqlite3.connect(db_file)
    con = conn.cursor()
    res = con.execute("select * from ASSIGN2")
    ret = res.fetchall()
    conn.commit()
    conn.close()
    if len(ret) == 0:
      data_to_insert = get_indicator_data(new_request['indicator_id'])
      if data_to_insert == 'Invalid indicator':
        return { 'message': 'Please specify a valid indicator.' }, 400
      ty = []
      for i in data_to_insert:
        ty.append(format_entries(i))
      collectiee_id = str(datetime.datetime.today())
      if collectiee_id not in STORE_collection_id:
        STORE_collection_id.append(collectiee_id)
        collectied_id = str(STORE_collection_id.index(collectiee_id))
      format_collection = {
        'indicator': data_to_insert[0]['indicator']['id'],
        'indicator_value': data_to_insert[0]['indicator']['value'],
        'creation_time': str(datetime.datetime.today()),
        'collection': collectied_id,
        'entries': ty
        }
      
      conn = sqlite3.connect(db_file)
      con = conn.cursor()
      con.execute("insert into ASSIGN2 values(?,?,?,?,?)",(format_collection['indicator'],format_collection['indicator_value'],format_collection['creation_time'],format_collection['collection'],str(format_collection['entries'])))
      conn.commit()
      conn.close()
      return{
        'location': f'/{COLLECTION}/{collectied_id}',
        'collection_id': collectied_id,
        'creation_time': format_collection['creation_time'],
        'indicator': format_collection['indicator'],
        
        }, 201
    elif len(ret) >0:
      debug_q1 = False
      for ke in ret:
        if ke[0] == new_request['indicator_id']:
          yu = ke
          debug_q1 = True
          break
      if not debug_q1:
        data_to_insert = get_indicator_data(new_request['indicator_id'])
        if data_to_insert == 'Invalid indicator':
          return { 'message': 'Please specify a valid indicator.' }, 400
        ty = []
        for i in data_to_insert:
          ty.append(format_entries(i))
        collectiee_id = str(datetime.datetime.today())
        #decide the unique collected_id
        if collectiee_id not in STORE_collection_id:
          STORE_collection_id.append(collectiee_id)
          collectied_id = str(STORE_collection_id.index(collectiee_id))
          
        format_collection = {
          'indicator': data_to_insert[0]['indicator']['id'],
          'indicator_value': data_to_insert[0]['indicator']['value'],
          'creation_time': str(datetime.datetime.today()),
          'collection': collectied_id,
          'entries': ty
          }
        
        conn = sqlite3.connect(db_file)
        con = conn.cursor()
        con.execute("insert into ASSIGN2 values(?,?,?,?,?)",(format_collection['indicator'],format_collection['indicator_value'],format_collection['creation_time'],format_collection['collection'],str(format_collection['entries'])))
        conn.commit()
        conn.close()
        return{
          'location': f'/{COLLECTION}/{collectied_id}',
          'collection_id': collectied_id,
          'creation_time': format_collection['creation_time'],
          'indicator': format_collection['indicator'],
          
          }, 201
      elif debug_q1:       
        return{
          'location': f'/{COLLECTION}/{str(yu[3])}',
          'collection_id': str(yu[3]),
          'creation_time': str(yu[2]),
          'indicator': str(yu[0]),
          }, 200

  @api.doc(description='[Q3] Retrieve the list of available collections.')
  @api.response(200, 'Successfully find collections.')
  @api.response(400, 'Unable to find collections.')
  def get(self):
    conn = sqlite3.connect(db_file)
    con = conn.cursor()
    res = con.execute("select * from ASSIGN2")
    ret = res.fetchall()
    conn.commit()
    conn.close()
    store_result_q3 = []
    if len(ret) == 0:
      return { 'message': 'Unable to find collections.' }, 400
    else:
      for j in range(len(ret)):
        uno_q2 = ret[j]
        result_q3={
          'location':f'/{COLLECTION}/{str(uno_q2[3])}',
          'collection_id': f'collection_id_{j+1}',
          'creation_time': str(uno_q2[2]),
          'indicator': str(uno_q2[0]),
          }
        
        store_result_q3.append(result_q3)
      return store_result_q3,200
@api.route(f'/{COLLECTION}/<collection_id>', endpoint=f'{COLLECTION}_by_id')
@api.param('collection_id', f'Unique id, used to distinguish {COLLECTION}.')
class CollectionsById(Resource):
  @api.doc(description='[Q2] Deleting a collection with the data service.')
  @api.response(200, 'Successfully removed collection.')
  @api.response(404, 'Unable to find collection.')
  @api.response(400, 'Unable to remove collection.')
  def delete(self, collection_id):
    collection_id_q3 = str(collection_id)
    debug_q3 = False
    conn = sqlite3.connect(db_file)
    con = conn.cursor()
    res = con.execute("select collection from ASSIGN2")
    ret = res.fetchall()
    conn.commit()
    conn.close()
    if len(ret) == 0:
      return { 'message': 'Unable to find collection.' }, 404
    elif len(ret) >0:
      for m in ret:
        if collection_id_q3 in m:
          debug_q3 = True
          break
      if not debug_q3:
        return { 'message': 'Unable to find collection.' }, 404
      try:
        conn = sqlite3.connect(db_file)
        con = conn.cursor()
        con.execute("delete from ASSIGN2 where collection = ?",(collection_id_q3,))
        conn.commit()
        conn.close()
      except:
        return { 'message': 'Unable to remove collection.' }, 400
      return { 'message': f'Collection = {collection_id} has been removed from the database!' }, 200
  @api.doc(description='[Q4] Retrieve a collection.')
  @api.response(200, 'Successfully retreived collection.')
  @api.response(404, 'Unable to retreive collection.')
  def get(self,collection_id):
    collection_id_q4 = str(collection_id)
    conn = sqlite3.connect(db_file)
    con = conn.cursor()
    res = con.execute("select collection from ASSIGN2")
    ret = res.fetchall()
    conn.commit()
    conn.close()
    if len(ret)==0:
      return { 'message': 'Unable to find collection' }, 404
    elif len(ret)>0:
      uo_q4 = 0
      debug_q4 = False
      for u in range(len(ret)):
        if collection_id_q4 in ret[u]:
          uo_q4 = u
          debug_q4 = True
          break
      if not debug_q4:
        return { 'message': 'Unable to find collection' }, 404
      conn = sqlite3.connect(db_file)
      con = conn.cursor()
      ress_q4 = con.execute("select * from ASSIGN2")
      rett_q4 = ress_q4.fetchall()
      conn.commit()   
      conn.close()
      yu_q4 = rett_q4[uo_q4]
      return{
        'collection_id': str(yu_q4[3]),
        'indicator': str(yu_q4[0]),
        'indicator_value': str(yu_q4[1]),
        'creation_time': str(yu_q4[2]),
        'entries': [li_q4 for li_q4 in eval(yu_q4[4])],
        },200
    
@api.route(f'/{COLLECTION}/<collection_id>/<year>/<country>', endpoint=f'{COLLECTION}_countrydate')
@api.param('collection_id', f'Unique id, used to distinguish {COLLECTION}.')
@api.param('year', 'Year ranging from 2013 to 2018.')
@api.param('country', 'Country identifier')
class CollectionByCountryYear(Resource):
  @api.doc(description='[Q5] Retrieve economic indicator value for given a country and year.')
  @api.response(200, 'Successfully retrieved economic indicator for given a country and year.')
  @api.response(400, 'Unable to retrieve indicator entry.')
  @api.response(404, 'Unable to find collection.')
  def get(self, collection_id, year, country):
    collection_q5 = str(collection_id)
    year_q5 = str(year)
    country_q5 = str(country)
    collection_id_q4 = str(collection_id)
    conn = sqlite3.connect(db_file)
    con = conn.cursor()
    res = con.execute("select collection from ASSIGN2")
    ret = res.fetchall()
    ress = con.execute("select * from ASSIGN2")
    rett = ress.fetchall()
    conn.commit()
    conn.close()
    if len(ret) == 0:
        return{ 'message': 'Unable to find collection' }, 404
    elif len(ret)>0:
        colindex_q5 = 0
        deb_col_q5 = False
        for col_q5 in range(len(ret)):
            if collection_q5 in ret[col_q5]:
                colindex_q5 = col_q5
                deb_col_q5 = True
                break
        if not deb_col_q5:
            return{ 'message': 'Unable to find collection' }, 404
        if deb_col_q5:
            traget_index_q5 = 0
            debug_q5 = False
            entry_q5 = eval(rett[colindex_q5][4])
            for dicc_q5 in range(len(entry_q5)):
                if entry_q5[dicc_q5]['country'] == country_q5 and entry_q5[dicc_q5]['date'] == year_q5:
                    traget_index_q5 = dicc_q5
                    debug_q5 = True
                    break
            if not debug_q5:
                return{'message': 'Unable to find specific indicator entry '+f'for country=\'{country_q5}\' and year=\'{year_q5}\'.'}, 400
            else:
                uno_q5 = rett[colindex_q5]
                return{
                    'collection_id': str(uno_q5[3]),
                    'indicator': str(uno_q5[0]),
                    'country': str(entry_q5[traget_index_q5]['country']),
                    'year': str(entry_q5[traget_index_q5]['date']),
                    'value': str(uno_q5[1]),
                    },200
@api.route(f'/{COLLECTION}/<collection_id>/<year>', endpoint=f'{COLLECTION}_by_top_bottom')
@api.param('collection_id', f'Unique id, used to distinguish {COLLECTION}.')
@api.param('year', 'Year ranging from 2013 to 2018.')
class CollectionByTopBottom(Resource):
  @api.doc(description='[Q6] Retrieve top/bottom economic indicator values for a given year.')
  @api.response(200, 'Successfully retreived economic indicator values.')
  @api.response(400, 'The provided parameter value is not valid')
  @api.response(404, 'Unable to find collection.')
  @api.expect(parser)
  def get(self, collection_id, year):
      input_argument_q6 = parser.parse_args()
      query = input_argument_q6['query']
      direct = ''
      amount = 0
      collection_q6 = str(collection_id)
      year_q6 = str(year)
      conn = sqlite3.connect(db_file)
      con = conn.cursor()
      res = con.execute("select collection from ASSIGN2")
      ret = res.fetchall()
      ress = con.execute("select * from ASSIGN2")
      rett = ress.fetchall()
      conn.commit()
      conn.close()
      if len(ret) == 0:
          return{ 'message': 'Unable to find collection' }, 404
      elif len(ret)>0:
          colindex_q6 = 0
          deb_col_q6 = False
          for col_q6 in range(len(ret)):
            if collection_q6 in ret[col_q6]:
                colindex_q6 = col_q6
                deb_col_q6 = True
                break
          if not deb_col_q6:
              return{ 'message': 'Unable to find collection' }, 404
          if deb_col_q6:#collection_id is good
              entry_q6 = eval(rett[colindex_q6][4])
              debug_year_q6 = False
              for ye_q6 in entry_q6:
                  if ye_q6['date'] == year_q6:
                      debug_year_q6 = True
                      break
              if not debug_year_q6:
                  return{ 'messgae': 'The provided parameter value is not valid'}, 400
              if debug_year_q6:# year is good
                  uno_q6 = rett[colindex_q6]
                  if query == None:
                      return{
                          'indicator': str(uno_q6[0]),
                          'indicator_value': str(uno_q6[1]),
                          'entries': [li_q6 for li_q6 in entry_q6 if li_q6['date'] == year_q6 ],
                          }, 200
                  elif 'top' in query:
                      direct = 'top'
                      amount = int(query.replace('top',''))
                      right_entry = sort_entries(entry_q6,year_q6,direct,amount)
                      return{
                          'indicator': str(uno_q6[0]),
                          'indicator_value': str(uno_q6[1]),
                          'entries': right_entry,
                          }, 200
                  elif 'bottom' in query:
                      direct = 'bottom'
                      amount = int(query.replace('bottom',''))
                      right_entry = sort_entries(entry_q6,year_q6,direct,amount)
                      return{
                          'indicator': str(uno_q6[0]),
                          'indicator_value': str(uno_q6[1]),
                          'entries': right_entry,
                          }, 200
      
            
if __name__ == '__main__':
  create_db(db_file)
  app.run(debug=DEBUG)











