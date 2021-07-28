#!/usr/bin/env python3

import psycopg2 as pg

def delete_registers(table, filters, credentials: str, verbose: bool = False):
    
    if table == "":
        return

    conn = pg.connect(credentials)
    cursor = conn.cursor()

    where = ''
    if filters:
        where = f' WHERE {filters} '

    query = f"""DELETE FROM {table} {where};"""
    if verbose:
        print(query)

    cursor.execute(query)
    
    conn.commit()
    cursor.close()
    conn.close()