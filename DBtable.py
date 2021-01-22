def creat_dailysqltable(cur,engine,Sysblem):
    name_Table="DailyD"+Sysblem
    sqlCreateTable ="create table "+name_Table+" (close_price FLOAT, high_price FLOAT, low_price FLOAT, open_price FLOAT,s varchar(255),t INT,volume INT);"
    cur.execute(sqlCreateTable)
    engine.commit()
    return name_Table


def uploaddata_daily(cur,name_Table,df_D):
    cols = ",".join([str(i) for i in df_D.columns.tolist()])
    table_name='DailyD'
    # Insert DataFrame recrds one by one.
    for i,row in df_D.iterrows():
        sql = "INSERT INTO " +table_name+" (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
        cur.execute(sql, tuple(row))
