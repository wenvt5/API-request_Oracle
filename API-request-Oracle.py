# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 13:06:19 2018

@author: yingw
"""

import json
import time
import requests
import pandas as pd
import cx_Oracle
import os
import sqlalchemy as sa

db_source='production'
class JobException(Exception):
    pass


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

db_info = pd.read_csv("dbInfo.csv")
db_user=db_info.loc[db_info['database']==db_source]['username'].values[0]
db_password=db_info.loc[db_info['database']==db_source]['password'].values[0]
database_alias=db_info.loc[db_info['database']==db_source]['db_string'].values[0]
con = cx_Oracle.connect("{}/{}@{}".format(db_user, db_password, database_alias))
cur = con.cursor()
query = "select ntp_tdms_number,sex,organ,morph,dose,tumor_count,subject_count from rpts_p8p10_stats where NTP_TDMS_NUMBER = '05132-05' and SEX='MALE' and ORGAN='Larynx' and MORPH='Metaplasia'"
study=pd.read_sql_query(query, con)
cur.close()
con.close()


study_summary = study.groupby(['NTP_TDMS_NUMBER','ORGAN','MORPH','SEX']).size()
study_summary_for_BMD = study_summary[study_summary>2]
BMD_value = pd.Series([])
for i in range(len(study_summary_for_BMD)): #study_summary_for_BMD.size):
    tdms_number = study_summary_for_BMD.index[i][0]
    organ = study_summary_for_BMD.index[i][1]
    morph = study_summary_for_BMD.index[i][2]
    sex = study_summary_for_BMD.index[i][3]
    study_group = study.loc[(study['NTP_TDMS_NUMBER']==tdms_number) & (study['ORGAN']==organ) & (study['MORPH']==morph) & (study['SEX']==sex)]
    if study_group.loc[study_group['DOSE']==0].shape[0] == 1:
        doses = study_group['DOSE']
        ns = study_group['SUBJECT_COUNT']
        incidences = study_group['TUMOR_COUNT']
        result = run_job(
                'https://sandbox-staging.ntp.niehs.nih.gov/job-runner/api/v1/bmds-dose-response/',

                {
                "id": "Ying's BMDS-server run",
                "dataset_type": "D",
                "bmds_version": "BMDS270",
                "datasets": [
                        {
                                "id": "test signle p8p10",
                                "doses": doses.tolist(),
                                "ns": ns.tolist(),
                                "incidences": incidences.tolist()
                        }


                            ]
                },
    'abcdefghijklmnopqrstuvwxyz1234567891',

        )

    job_id = result['job_id']
    excel_url = "http://ehsbmdvwd03/api/job/" + job_id + "/excel/"
    r = requests.get(excel_url, allow_redirects=True)
    print (r.headers.get('content-type'))
    os.chdir('C:\\Users\\yingw\\Desktop\\Ying\\BMDS_p8p10\\excel_results')
    file_name = tdms_number + '_' + organ + '_' + morph + '_' + sex +'.xlsx'
    #open(file_name, 'wb').write(r.content)
    with open(file_name,'wb') as f:
        f.write(r.content)

    excel_dataframe = pd.read_excel(file_name, sheet_name='models')
    excel_dataframe['sex'] = sex
    excel_dataframe['morph'] = morph
    excel_dataframe['organ'] = organ
    excel_dataframe['ntp_tdms_number'] = tdms_number



    db_source='Stage'
    db_user=db_info.loc[db_info['database']==db_source]['username'].values[0]
    db_password=db_info.loc[db_info['database']==db_source]['password'].values[0]
    database_alias=db_info.loc[db_info['database']==db_source]['db_string'].values[0]
    con = cx_Oracle.connect("{}/{}@{}".format(db_user, db_password, database_alias))
    cur = con.cursor()
    columns_query = "select * from test_test_yw where rownum <= 5"
    columns = pd.read_sql_query(columns_query, con)


    test_excel_dataframe = excel_dataframe.drop(columns=['dataset_id']) #[['ntp_tdms_number','organ','morph','sex','outfile']]
    test_excel_dataframe.columns = map(str.upper, test_excel_dataframe.columns)
    test_excel_dataframe = test_excel_dataframe[columns.columns.tolist()]



    # insert single row to data table, works!
    con = cx_Oracle.connect("{}/{}@{}".format(db_user, db_password, database_alias))
    cur = con.cursor()
#    list_columns_names = str(list(test_excel_dataframe.columns.values))
    excel_sql = "insert into TEST_TEST_YW(NTP_TDMS_NUMBER, ORGAN, MORPH, SEX) VALUES('00602-01','Bone Marrow','Myelofibrosis', 'FEMALE')"
    cur.execute(excel_sql)
    con.commit()
    cur.close()

    # insert many rows by using executemany, does not work!
    test_excel_list = test_excel_dataframe.values.tolist()
    new_list = [tuple(row) for row in test_excel_list]
    cur.executemany(excel_sql, new_list[0:1])

    
    ## insert many rows by using Engine, works!
    #oracle_db = sa.create_engine('oracle://' + db_user + ':' + db_password + '@' + database_alias2 ) #only for TNS
    oracle_db = sa.create_engine('oracle://' + db_user + ':' + db_password + '@' + '(DESCRIPTION = (LOAD_BALANCE=on) (FAILOVER=ON) (ADDRESS = (PROTOCOL = TCP)(HOST =' + 'ehsoravld04.niehs.nih.gov' + ')(PORT = 1521)) (CONNECT_DATA = (SERVER = DEDICATED) (SERVICE_NAME =' + 'EHSINT1.niehs.nih.gov)))')
    con = oracle_db.connect()
    test_excel_dataframe.to_sql('TEST_TEST_YW',con ,if_exists = 'append', index=False)
    con.close()
