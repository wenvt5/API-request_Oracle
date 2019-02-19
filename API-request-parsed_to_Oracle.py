# -*- coding: utf-8 -*-
"""
Created on Wed Jun 27 13:06:19 2018
@author: yingw
This program has the following functions:
1. read login info from a file and make connection to a specified Oracle database
2. query data from the connection and reformat it to suit API requests
3. make the api request with the data prepared from the previous step
4. save the retured excel file
5. parse the result to dataframe matching database table
6. write the dataframe format of result to Oracle datatable

The following functions have been refactored to python class "OracleManager" in read_write_Oracle.py:
1. read login info from a file and make connection to a specified Oracle database
2. query data from the connection
3. write the dataframe to Oracle datatable

"""

import OracleManager from read_write_Oracle


# query from production database
db_source='production'
query = "select blabla from blabla where blabla"

# initiate a instance
OracleConnect = OracleManager()

# query results
study = OracleConnect.query_from_Oracle(query)

# parse the query result to the right format for API request, and then make the API request
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
        result = OracleManage.run_job(
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

    # download and save the generated excel file
    sult['job_id']
    excel_url = "http://ehsbmdvwd03/api/job/" + job_id + "/excel/"
    r = requests.get(excel_url, allow_redirects=True)
    print (r.headers.get('content-type'))
    os.chdir('C:\\Users\\yingw\\Desktop\\Ying\\excel_results')
    file_name = tdms_number + '_' + organ + '_' + morph + '_' + sex +'.xlsx'
    #open(file_name, 'wb').write(r.content)
    with open(file_name,'wb') as f:
        f.write(r.content)

    # prepare the data from excel ready to be write to Oracle db
    excel_dataframe = pd.read_excel(file_name, sheet_name='models')
    excel_dataframe['sex'] = sex
    excel_dataframe['morph'] = morph
    excel_dataframe['organ'] = organ
    excel_dataframe['ntp_tdms_number'] = tdms_number

    # only keep the columns matching the datatable that we are writing to
    db_source='Stage'
    columns_query = "select * from test_test_yw where rownum <= 5"
    OracleConnect = OracleManager()
    columns = OracleConnect.query_from_Oracle(db_source, columns_query)
    test_excel_dataframe = excel_dataframe.drop(columns=['dataset_id']) #[['ntp_tdms_number','organ','morph','sex','outfile']]
    test_excel_dataframe.columns = map(str.upper, test_excel_dataframe.columns)
    test_excel_dataframe = test_excel_dataframe[columns.columns.tolist()]

    # insert single row to data table
    OracleConnect = OracleManager()
#    list_columns_names = str(list(test_excel_dataframe.columns.values))
    excel_sql = "insert into TEST_TEST_YW(columns list) VALUES(columns values)"
    OracleConnect.write_single_row_Oracle(excel_sql)


    ## insert many rows by using Engine
    OracleManage.write_mang_rows_Oracle(test_excel_dataframe)
