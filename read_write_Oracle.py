#-*- coding: utf-8 -*-
"""Classes for connecting, querying and writing Oracle db."""

import sqlalchemy as sa
import cx_Oracle
import os
import json
import time
import requests
import pandas as pd

class OracleManager:
    """Manage connecting, query from Oracle and writing results to Oracle."""

    def __init__(self, db_source):
        """Connect to a Oracle database."""
        db_info = pd.read_csv("dbInfo.csv")
        db_user=db_info.loc[db_info['database']==db_source]['username'].values[0]
        db_password=db_info.loc[db_info['database']==db_source]['password'].values[0]
        database_alias=db_info.loc[db_info['database']==db_source]['db_string'].values[0]
        con = cx_Oracle.connect("{}/{}@{}".format(db_user, db_password, database_alias))
        self.con = con

    def query_from_Oracle(self, query):
        """Query data from created connection."""
        cur = self.con.cursor()
        study=pd.read_sql_query(query, self.con)
        cur.close()
        self.con.close()
        return study

    def write_single_row_Oracle(self, data):
        """Insert single row data to created connection."""
        cur = self.con.cursor()
        cur.execute(data)
        self.con.commit()
        cur.close()
        self.con.close()

    @staticmethod
    def write_mang_rows_Oracle(data):
        """Write many rows data to created connection."""
        db_info = pd.read_csv("dbInfo.csv")
        db_user=db_info.loc[db_info['database']==db_source]['username'].values[0]
        db_password=db_info.loc[db_info['database']==db_source]['password'].values[0]
        oracle_db = sa.create_engine('oracle://' + db_user + ':' + db_password + '@' + '(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST =' + 'host url' + ')(PORT = xxx)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME =' + 'service name)))')
        con = oracle_db.connect()
        data.to_sql('TEST_TEST_YW',con ,if_exists = 'append', index=False)
        con.close()

    @staticmethod
    def run_job(url, data, api_token, interval=3, timeout=60):
        with requests.Session() as s:
            s.headers.update({
                "Authorization": f"Token {api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            })

            # initial job request
            response = s.post(url, data=json.dumps(data))
            payload = response.json()
            if response.status_code in [400, 403]:
                raise JobException(payload['detail'])

            # handle result if finished
            if payload['is_finished']:
                if payload['has_errors']:
                    raise JobException(payload['errors'])
                else:
                    return payload['outputs']

            # poll response until job is complete or client-timeout
            wait_time = 0
            url = payload['url']
            while True:
                time.sleep(interval)
                payload = s.get(url).json()

                # handle result if finished
                if payload['is_finished']:
                    if payload['has_errors']:
                        raise JobException(payload['errors'])
                    else:
                        return payload['outputs']

                wait_time += interval
                if wait_time > timeout:
                    raise JobException('Client timeout')
