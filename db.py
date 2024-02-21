import os
import pandas as pd
import pyodbc
import shutil
import time

def get_blank_file(dest_path: str) -> None:
    tmp_path = os.path.abspath(os.path.join('input', 'blank_template.gpj'))    
    shutil.copyfile(tmp_path, dest_path)

def get_cursor(filepath: str) -> pyodbc.Cursor:
    
    conn_str = (
    r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' +
    filepath +';')
    cnxn = pyodbc.connect(conn_str)
    cursor = cnxn.cursor()
    return cursor

def get_db_tables(c: pyodbc.Cursor):
    return [t.table_name for t in c.tables(tableType='TABLE')]

def get_tables_decriptions(c: pyodbc.Cursor, table: str):
    _check_table_in_db(c, table)
    sql_query = f"SELECT * FROM [{table}]"
    c.execute(sql_query)
    return [column_info for column_info in c.description]


def _check_table_in_db(c: pyodbc.Cursor, table: str) -> None:
    if table not in get_db_tables(c):
        raise KeyError(f'Tablle [{table}] not in db.')

def get_attrs_in_table(c: pyodbc.Cursor, table: str) -> list:
    """
    Returns the columns in a table.
    
    Args:
        table (str): Table of interest.
    """
    _check_table_in_db(c, table)
    sql_query = f"SELECT * FROM [{table}]"
    c.execute(sql_query)
    return [column_info[0] for column_info in c.description]

def query_table(c: pyodbc.Cursor, table: str) -> list[dict]:
    """
    """
    _check_table_in_db(c, table)
    attrs = get_attrs_in_table(c, table)

    # Generate Query
    f = ", ".join([f'[{c}]' for c in attrs])    
    sql_query = "SELECT " + f + " FROM " + f"[{table}]"

    c.execute(sql_query)
    return c.fetchall()

def insert_records_to_table(
        c: pyodbc.Cursor,
        table: str,
        data: pd.DataFrame,
    ):
    """
    Inserts a list of records into ``table``. Recall SQL syntax:
    
    INSERT INTO table_name (column1, column2, column3, ...)
        VALUES (value1, value2, value3, ...);

    Args:
        c (pyodbc.Cursor): Cursor  
        table (str): SQL table
        data (pd.DataFrame): Records to be inserted.
    """
    # Dynamically create columns part of the query
    cols = ''
    for col in data.columns:
        if ' ' not in col:
            cols = cols + col + ', '
        else: 
            cols = cols + "[" + col + "]" + ', '
    cols = cols[:-2]

    # Dynamically create parms part of the query
    params = ''
    for _ in data.columns:
        params += '?, '
    params = params[:-2]

    sql_query = f'insert into {table} ({cols}) values ({params})'

    for index, row in data.iterrows():
        # print(len(row.values))
        c.execute(sql_query, row.to_list())
        c.commit()

# filepath = r'.\test1.gpj'
# c = get_cursor(filepath)
# tables = get_db_tables(c)
# # location_headers = get_attrs_in_table(c, 'POINT')
# # print(location_headers)
# print(query_table(c, 'POINT'))

# # c.execute('''
# #     insert into POINT (PointID, HoleDepth, Elevation) values ('HA-01', '100', '765')
# # '''
# # )
# # c.commit()

# # point_id, depth, elevation = 'HA-04', 364, None
# # c.execute('''
# #     insert into POINT (PointID, HoleDepth, Elevation) values (?, ?, ?)
# # ''', (point_id, depth, elevation)
# # )
# # c.commit()


# # get_blank_file(filepath)


# # tables = [t.table_name for t in cursor.tables(tableType='TABLE')]
