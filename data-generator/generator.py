from google.cloud import secretmanager
import argparse
import pymysql
import logging
import random

class AssetDB():
    def __init__(self, host, user, password, db):
        # self.host = host
        # self.user = user
        # self.password = password
        # self.db = db
        self.connection = pymysql.connect(host=host, user=user, password=password, database=db)

    def execute(self, sql, args=None):
        with self.connection.cursor() as cursor:
            cursor.execute(sql, args)
        self.connection.commit()

    def executemany(self, sql, args):
        with self.connection.cursor() as cursor:
            cursor.executemany(sql, args)
        self.connection.commit()

    def init_db(self, symbols, user_id_range=(1,100000)):
        for i in range(user_id_range):
            sql = "insert into user_asset (user_id, asset, balance) values (%s, %s, %s)"
            args = []
            for symbol in symbols:
                arg = (i, symbol, round(random.uniform(1.0, 100.0), 8))
                args.append(arg)
            self.executemany(sql, args)

def get_password(project_id, secret_id):
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": f"projects/{project_id}/secrets/{secret_id}/versions/latest"})
    payload = response.payload.data.decode("UTF-8")
    return payload

def get_symbols():
    with open("symbols.txt", "r") as fin:
        lines = fin.read()
        return lines.split("\n")
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--project', help='project id')
    parser.add_argument('--secret', help='secret id that contains db password')
    parser.add_argument('--db_host', help='db host ip or hostname')
    parser.add_argument('--db_user', help='db username')
    parser.add_argument('--db_schema', help='db schema')
    parser.add_argument('--init', action='store_true', help='init db')

    args = parser.parse_args()
    project_id = args.project
    secret_id = args.secret
    db_host = args.db_host
    db_user = args.db_user
    db_password = get_password(project_id, secret_id)
    symbols = get_symbols()
    
    print(db_password)
    print(symbols)
    # db = AssetDB(db_host, db_user, db_password, db_schema)
    # db.execute("select 1")
    
    if args.init:
        logging.info("Initializing DB...")
        db.init_db(symbols)
        logging.info("DB initialized. Exit.")
        exit(0)
    