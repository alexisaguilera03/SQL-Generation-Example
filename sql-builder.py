
from ast import Str
import copy
import copy
import math
import re
import datetime
import pandas as pd
import os
import multiprocessing





def createQuery(file: str, table: str, columns: list):
    rex = "\B'\w+'\B",
    issue = list()
    #location = os.getcwd() + f"\\Create Sql\\{file}.xlsx"
    location = os.getcwd() + f"\\{file}.xlsx"
    sql = f"\tSET IDENTITY_INSERT dbo.{table} ON;\n\tINSERT INTO dbo.{table} ("
    df = pd.read_excel(location)
    for i in range(0, len(df.columns)):
        if i == len(df.columns[i]) -1 :
            sql = sql + f"{df.columns[i]}"
            break
        sql = sql + f"{df.columns[i]}, "
    sql = sql + ")\n\tVALUES\n"
    for i in range(0, len(df.values)):
    #6for i in range(1000,4000):
        if i % 100 == 0:
            print(f"Progress: row {i}\nOverall Progress: {math.floor(i/len(df.values)*100)}%")
        if i == 0: sql = sql + f"\t\t("
        else: sql = sql + f"\t\t,("
        
        for j in range(0, len(df.values[i])):
            
            if type(df.values[i][j]) is not str and type(df.values[i][j]) is not int:
                if type(df.values[i][j]) == float and math.isnan(df.values[i][j]):
                    sql = sql + "NULL, "
                    continue
                if type(df.values[i][j]) == float:
                    sql = sql + f"{int(df.values[i][j])}, "
                    continue
                if type(df.values[i][j]) == pd._libs.tslibs.timestamps.Timestamp:
                    sql = sql + f"CAST('{str(df.values[i][j])}' AS DATETIME2), "
                    issue.append(f"CAST('{str(df.values[i][j])}' AS DATETIME2)")
                    continue
                if type(df.values[i][j]) == pd._libs.tslibs.nattype.NaTType:
                    sql = sql + "NULL, "
                    continue
                else:
                    sql = sql + f"'{str(df.values[i][j]).strip()}', "
                    continue

            if j == len(df.values[i]) - 1:
                try:
                    sql = sql + f"{int(df.values[i][j])}"
                    continue
                except:
                    sql = sql + f"'{df.values[i][j].strip()}'"
                    continue
                #this if statement is to deal with bpart having mixed data types. 
            if j in columns:
                sql = sql + f"{int(df.values[i][j])},"
                continue
            else:
                if str(df.values[i][j]).strip() == '':
                    sql = sql + "'', "
                    continue
                tmp = str(df.values[i][j]).strip().replace("'", "''")
                sql = sql + f"'{str(tmp)}', "
                continue
            try:
                sql = sql + f"{int(df.values[i][j])}, "
            except:
                if df.values[i][j].strip() == '':
                    sql = sql + "'', "
                    continue
                tmp = df.values[i][j].strip().replace("'", "''")
                sql = sql + f"'{tmp}', "
        if sql[-1] == ',':
            sql = sql[0:-2]
        if sql[-2] == ',':
            sql = sql[0:-2]
        sql = sql + ")\n"
    sql = sql[0:-1] + ";"
    w = open(f"{file}.txt", "w")
    w.write(f"DELETE FROM dbo.{table}\nGO\nBEGIN TRY\n\tBEGIN TRANSACTION\n")
    w.write(sql)
    w.write(f"\n\t\tCOMMIT;\nEND TRY\nBEGIN CATCH\n\tIF @@TRANCOUNT > 0\n\t\tROLLBACK;\nSET IDENTITY_INSERT dbo.{table} OFF;\n\tTHROW;\nEND CATCH\nSET IDENTITY_INSERT dbo.{table} OFF;")
    w.close()
    if len(issue) > 0:
        w = open("test.txt", "w")
        for i in issue:
            w.write(f"SELECT {i}\n\n")
        w.close()
    return 0
            

def getData():
    location = os.getcwd() + f"\\Create Sql\\BPART_load_file.xlsx"
    df = df = pd.read_excel(location)
    return df





def createModifiedQuery():
    file = open(os.getcwd() + '\\BPART_load_file.txt', 'r')
    w = open(os.getcwd() + '\\newQuery.txt', 'w')
    counter = 1
    written = False
    lines = file.readlines()
    sql = f"DELETE FROM dbo.BPART\nGO\nBEGIN TRY\n\tBEGIN TRANSACTION\n"
    for i in range(0, len(lines)):
        if i == 0 or counter == 1: 
            sql += f"\tINSERT INTO dbo.BPART (BPART_NR, BPART_NM, BPART_TYPE_CD, DOE_NR, NON_DOE_ID_TX, ENTR_GP, LAST_WEB_UID_TX, LAST_UPDT_TS, COMZN_ID_TX, DOE_ALPHA_SRCH_TX, INDIR_NR)\n\tVALUES\n"
        if counter == 1000:
            written = True
            w.write(sql)
            w.write(f"\n\t\tCOMMIT;\nEND TRY\nBEGIN CATCH\n\tIF @@TRANCOUNT > 0\n\t\tROLLBACK;\nSET IDENTITY_INSERT dbo.BPART OFF;\n\tTHROW;\nEND CATCH\nSET IDENTITY_INSERT dbo.BPART OFF;\n\n")
            sql = "SET IDENTITY_INSERT dbo.BPART ON\nBEGIN TRY\n\tBEGIN TRANSACTION\n"
            counter = 1
            i -=1
            continue
        
        if written == True and lines[i][2] == ',' and counter == 1:
           # sql += findNumbers(lines[i][3:-1])
            sql = sql + lines[i][3:-1]
            counter += 1
            continue
        #sql = sql + findNumbers(lines[i])
        sql = sql + lines[i]
        counter +=1    
    file.close()
    w.close()


if __name__ == '__main__':
    files = ['ACHACT_load_file', 'ACHB58_load_file', 'ACHBAD_load_file', 'ACHBAT_load_file', 'ACHF19_load_file', 'ACHRTG_load_file', 'ACHTRN_load_file', 'CALDR_load_file', 'CODE_load_file', 'D224P_load_file', 'BPART_load_file']
    tables = ['ACHACT', 'ACHB58', 'ACHBAD', 'ACHBAT', 'ACHF19', 'ACHRTG', 'ACHTRN',  'CALDR', 'CODE', 'D224P', 'BPART',]
    print("Starting")
    test = list()
    test.append([6,7,8,9,10,11])
    createQuery(files[4], tables[4], test)
    createModifiedQuery()
    exit(0)
    for i in range(0, len(files)-1):
        print(f"Starting file: {files[i]}")
        createQuery(files[i], tables[i])
    exit(0)
