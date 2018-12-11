
import pymssql # http://pymssql.org/en/latest/index.html 
from Setting import Config as conf

def ExecuteQuery(mConn,strSQLCmd,dictParam_with_query = ""):
    try:
        cursor = mConn.cursor()
        if (dictParam_with_query == ""):
            cursor.execute(strSQLCmd)
        else:
            cursor.execute(strSQLCmd, dictParam_with_query)
        
        #print( cursor.fetchall() )  # shows result from query!
        #print( cursor.fetchone() )  # shows one result from query!
        return cursor 

    except Exception as e:
        print(str(e))

