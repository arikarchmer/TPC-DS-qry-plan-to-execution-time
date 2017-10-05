
import pyodbc
import time
    
def restart_connection(conn):
    
    conn.close()
    return create_connection()

    
def create_connection():
    
    conn_str = (
        "DRIVER=../../../../Library/PostgreSQL/psqlODBC/lib/psqlodbcw.so;"
        "DATABASE=postgres;"           
        "UID=postgres;"
        "PWD=b;"
        "SERVER=localhost;"
        "PORT=5432;"
    )
    
    conn = pyodbc.connect(conn_str)
    conn.setencoding('utf-8')
    return conn


def query(q_num, q, conn):
    
    try:
        crsr = conn.execute(q)
        return crsr.fetchall()
    except pyodbc.ProgrammingError:
        print('query ' + str(q_num) + ' excepted.')
    except pyodbc.Error:
        print('query ' + str(q_num) + ' timed out.')



def execute_query(conn, qry, explain=False):
    
    q_str = ''
    if explain:
        q_str = 'explain '
    
    with open('sqlFiles/query_' + str(qry) + '.sql', 'r') as q:
        for line in q:
            q_str += line + ' '
    
    res = query(qry, q_str, conn)
    
    if explain and res:
        for row in res:
            for b in row:
                print(b)


if __name__ == '__main__':
    
    queries = [3, 6, 7, 8, 9, 13, 15, 17, 
               18, 19, 22, 24, 25, 26, 27, 
               28, 29, 30, 31, 33, 34, 38, 
               39, 41, 42, 43, 44, 45, 46, 
               48, 49, 50, 51, 52, 53, 54, 
               55, 56, 57, 58, 59, 60, 61, 
               62, 63, 64, 65, 66, 67, 68, 
               69, 71, 72, 73, 75, 76, 78, 
               79, 81, 83, 84, 85, 87, 88, 
               89, 90, 91, 93, 96, 97]
    
    for q in queries:
        
        conn = create_connection()
        
        execute_query(conn, q, explain=True)
        
        begin = time.time()
        execute_query(conn, q, explain=False)
        end = time.time()
        print((q, end - begin))
        print()
        
        restart_connection(conn)
