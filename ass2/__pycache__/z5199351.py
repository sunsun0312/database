import pandas as pd
import sqlite3

from flask import Flask
from flask_restplus import Resource, Api
from flask_restplus import reqparse
import requests
import datetime



app = Flask(__name__)
api = Api(app)


def clean_db(db_file):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("DROP TABLE if exists Collections")
    cur.execute("DROP TABLE if exists Entries")
    conn.commit()
    conn.close()

# create a database with two tables: Collections and Entries
def create_db(db_file):
    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute('''CREATE Table if not exists Collections (id int,creation_time text,indicator_id text,indicator_value text)''')
    cur.execute('''CREATE Table if not exists Entries(id int, country text, year int,value real)''')

    conn.commit()


class Questions:
    def __init__(self):
        self.id = 0

    # Q1
    def import_db(self,indicator_id):
        # get the data
        url = f'http://api.worldbank.org/v2/countries/all/indicators/{indicator_id}?date=2012:2017&format=json&per_page=1000 '
        r = requests.get(url)

        if len(r.json()) <= 1:
            return False
        # transfer the data into dataframes
        banks = r.json()[1]
        df_banks = pd.DataFrame(banks)

        # drop the unnecessary columns
        columns_to_drop = ['countryiso3code','unit','obs_status','decimal']
        new_df_banks = df_banks.drop(columns_to_drop, axis=1)

        # get the correct columns of the dataframe
        new_df_banks['indicator_id'] = new_df_banks.indicator.apply(lambda x: x['id'])
        new_df_banks['indicator_value'] = new_df_banks.indicator.apply(lambda x: x['value'])
        # drop the rows if the value = 0 and reset the index
        new_df_banks['country'] = new_df_banks.country.apply(lambda x: x['value'])
        new_df_banks.dropna(subset = ['value'], inplace = True, axis = 0)
        new_df_banks.reset_index(drop = True,inplace=True)
        new_df_banks.drop("indicator",axis =1)

        new_banks = new_df_banks.to_dict(orient="records")

        # get the creation_time
        ctime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        # get the list of the all the indicator_id that already existed in the database
        result = cur.execute("select indicator_id from Collections").fetchall()
        ind_list = [i[0] for i in result]


        if indicator_id not in ind_list:
            # if the indicator_id is new, then update the id
            self.id += 1
            # get the data for the database
            data = {
                'id': self.id,
                'creation_time': ctime,
                'indicator_id': indicator_id,
                'indicator_value': new_df_banks['indicator_value'][0],
                'country': new_df_banks['country'],
                'year': new_df_banks['date'],
                'value': new_df_banks['value']
            }

            # insert data into two tables
            cur.execute("INSERT INTO Collections VALUES(?,?,?,?)", (data['id'], data['creation_time'], data['indicator_id'], data['indicator_value']))
            for i in range(len(new_banks)):
                cur.execute("INSERT INTO Entries VALUES(?,?,?,?)",(data['id'], new_banks[i]['country'], new_banks[i]['date'],new_banks[i]['value']))

            # return the message
            message = {
                "uri" : f"/collections/{self.id}",
                "id" : self.id,
                "creation_time": ctime,
                "indicator": indicator_id
            }

        # if the indicator_id already exists in the database, just show the details of the data
        sql = "select id,creation_time from Collections where indicator_id = ('%s')"%(indicator_id)
        cur.execute(sql)
        result2 = cur.fetchall()
        message = {
            "uri": f"/collections/{result2[0][0]}",
            "id": result2[0][0],
            "creation_time": result2[0][1],
            "indicator": indicator_id
        }

        conn.commit()
        conn.close()
        return message

    # Q2
    def delete_collection(self,id):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        # get all the id from the database
        cur.execute(f"select distinct id from Collections where id={id}")
        id_list = cur.fetchall()
        # if the databse is empty, return False
        if len(id_list) == 0:
            return False
        # if not empty, delete the data
        cur.execute(f"delete from Collections where id = {id}")

        conn.commit()
        conn.close()
        message = {
            "message" :f"The collection {id} was removed from the database!",
            "id": id
        }
        return message

    # Q3
    def retrieve_collections(self,order_by):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        result = []
        columns = ['id','indicator_id','creation_time','indicator_value','country','date','value']

        # judge if the input order_by string is valid
        if "+" not in order_by and "-" not in order_by:
            return False
        # if there is only one param to order by
        if "," not in order_by:
            if order_by[1:] not in columns:
                return False
            if "+" in order_by:
                cur.execute(f"select * from Collections order by {order_by[1:]} ASC")
                ret = cur.fetchall()
            if "-" in order_by:
                cur.execute(f"select * from Collections order by {order_by[1:]} DESC")
                ret = cur.fetchall()
            for i in ret:
                message = {
                    "uri": f"/collections/{i[0]}",
                    "id": i[0],
                    "creation_time": i[1],
                    "indicator": i[2]

                }
                result.append(message)
            new_result = result

        # if there are more than one params to order by
        if "," in order_by:
            orders = order_by.split(",")
            for i in orders:
                if all(j != i[1:] for j in columns):
                    return False
                if "+" in i:
                    cur.execute(f"select * from Collections order by {i[1:]} ASC")
                    ret = cur.fetchall()
                if "-" in i:
                    cur.execute(f"select * from Collections order by {i[1:]} DESC")
                    ret = cur.fetchall()
                for j in ret:
                    message = {
                        "uri": f"/collections/{j[0]}",
                        "id": j[0],
                        "creation_time": j[1],
                        "indicator": j[2]
                    }
                    result.append(message)

            limit = int(len(result) / len(orders))
            new_result = result[:limit]

        conn.commit()
        conn.close()
        return new_result

    # Q4
    def retrieve(self,id):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        # get the data from two tables
        c = cur.execute(f"select * from Collections where id = {id}").fetchall()
        e = cur.execute(f"select * from Entries where id = {id}").fetchall()
        # if the database is empty, return false
        if len(c) == 0:
            return False
        # create a list of entries, consists of dictories with 'country','data' and 'value'
        entries = []
        for i in e:
            result = {}
            result['country'] = i[1]
            result['date'] = i[2]
            result['value'] = i[3]
            entries.append(result)

        message = {
            "id": c[0][0],
            "indicator": c[0][2],
            "indicator_value": c[0][3],
            "creation_time": c[0][1],
            "entries": entries
        }

        conn.commit()
        conn.close()
        return message

    # Q5
    def retrieve_economic(self,id,year,country):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()

        e = cur.execute(f'select * from Entries where id = {id} and year = {year} and country = "{country}" ').fetchall()
        c = cur.execute(f'select indicator_id from Collections where id = {id}').fetchall()

        # judge if the database is empty
        if len(e) == 0:
            return False
        if len(c) == 0:
            return False
        message = {
            "id": id,
            "indicator": c[0][0],
            "country": country,
            "year": year,
            "value": e[0][3]
        }
        conn.commit()
        conn.close()
        return message

    # Q6
    def retrieve_top_bottom(self,id,year,q):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()

        c = cur.execute(f'select indicator_id,indicator_value from Collections where id = {id}').fetchall()
        ee = cur.execute(f'select * from Entries where id = {id} and year = {year}').fetchall()

        if len(c) == 0 or len(ee) == 0:
            return False
        # judge if the input query is valid
        if "+" not in q and "-" not in q:
            return 400
        if not str.isdigit(q[1:]):
            return 400

        entries = []
        if "+" in q:
            order = "DESC"

        elif "-" in q:
            order = "ASC"

        e = cur.execute(f"select * from Entries where id = {id} and year = {year} order by value {order}").fetchall()

        n = int(q[1:])

        for i in range(n):
            result = dict()
            result['country'] = e[i][1]
            result['value'] = e[i][3]
            entries.append(result)
        message = {
            "indicator": c[0][0],
            "indicator_value": c[0][1],
            "entries" : entries
        }
        return message


q = Questions()

parser1 = reqparse.RequestParser()
parser1.add_argument('indicator_id',type = str,location = 'args')
parser2 = reqparse.RequestParser()
parser2.add_argument('order_by',type = str,location = 'args')
parser3 = reqparse.RequestParser()
parser3.add_argument('q',type = str,location = 'args')

@api.route('/collections')
class Q1_Q3(Resource):
    # Q1
    @api.response(201,'Created')
    @api.response(404,'Error')
    @api.expect(parser1)
    def post(self):

        args = parser1.parse_args()
        indicator_id = args.get("indicator_id")
        result = q.import_db(indicator_id)
        if not result:
            return "Invalid Indicators!",404
        return result,201


    # Q3
    @api.response(200,'OK')
    @api.response(404,'Error')
    @api.response(400,'Bad Request')
    @api.expect(parser2)
    def get(self):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        ret = cur.execute("select id from Collections").fetchall()
        if ret == []:
            return "database is empty", 404
        args = parser2.parse_args()
        order_by = args.get("order_by")
        result = q.retrieve_collections(order_by)
        if not result:
            return "Bad Request",400
        return result,200

@api.route('/collections/<int:id>')
class Q2_Q4(Resource):
    # Q2
    @api.response(200, 'OK')
    @api.response(404, 'Error')
    def delete(self,id):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        ret = cur.execute("select id from Collections").fetchall()
        ind_list = [i[0] for i in ret]
        if id not in ind_list:
            return "id not exist",404
        result = q.delete_collection(id)
        return result,200

    # Q4
    @api.response(200, 'OK')
    @api.response(404, 'Error')
    def get(self, id):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        ret = cur.execute("select id from Collections").fetchall()
        if ret == []:
            return "database is empty", 404
        result = q.retrieve(id)
        if not result:
            return "input id does not exist",404
        return result, 200

@api.route('/collections/<int:id>/<int:year>/<country>')
class Q5(Resource):
    # Q5
    @api.response(200, 'OK')
    @api.response(404, 'Error')
    def get(self,id,year,country):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        ret = cur.execute("select id from Collections").fetchall()
        if ret == []:
            return "database is empty", 404
        result = q.retrieve_economic(id,year,country)
        if not result:
            return "input id year country may wrong",404
        return result,200

@api.route('/collections/<int:id>/<int:year>')
class Q6(Resource):
    # Q6
    @api.response(200, 'OK')
    @api.response(404, 'Error')
    @api.response(400, 'Bad Request')
    @api.expect(parser3)
    def get(self, id, year):
        conn = sqlite3.connect("z5199351.db")
        cur = conn.cursor()
        ret = cur.execute("select id from Collections").fetchall()
        if ret == []:
            return "database is empty", 404
        args = parser3.parse_args()
        query = args.get("q")
        result = q.retrieve_top_bottom(id,year,query)
        if not result:
            return "Wrong input",404
        if result == 400:
            return "Bad Request",400
        return result, 200

if __name__ == '__main__':
    clean_db("z5199351.db")
    create_db("z5199351.db")
    app.run(port = 5890,debug = True)

