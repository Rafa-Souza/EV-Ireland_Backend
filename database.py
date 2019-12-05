import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

connectionPool = None


def startDb():
    print('Starting Database...')
    createDB()
    createConnectionPool()
    createTables()
    print('Database Configured Successfully!')


def createConnectionPool():
    global connectionPool
    connectionPool = psycopg2.pool.SimpleConnectionPool(1, 20, user="postgres", password="admin",
                                                        host="127.0.0.1", port="5432", database="ev_db")


def createDB():
    conn = psycopg2.connect(
        user="postgres",
        password="admin",
        host="127.0.0.1",
        port="5432",
        database=""
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'ev_db'")
    exists = cursor.fetchone()
    if not exists:
        print('Creating Database...')
        cursor.execute('CREATE DATABASE ev_db')
    cursor.close()
    conn.close()


def createTables():
    conn = connectionPool.getconn()
    cursor = conn.cursor()
    print('Creating Tables...')
    cursor.execute('''CREATE TABLE IF NOT EXISTS charge_point (
        id SERIAL PRIMARY KEY,
        charge_point_id VARCHAR,
        latitude float,
        longitude float,
        address VARCHAR,
        type VARCHAR,
        unique (latitude, longitude, type)
    );''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS register_date (
        id SERIAL PRIMARY KEY,
        date TIMESTAMP UNIQUE
    );''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS register_status (
        id SERIAL PRIMARY KEY,
        fk_charge_point INTEGER REFERENCES charge_point,
        fk_register_date INTEGER REFERENCES register_date,
        status VARCHAR,
        unique (fk_charge_point, fk_register_date, status)
    );''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS file (
        id SERIAL PRIMARY KEY,
        name VARCHAR UNIQUE
    );''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS bulk_insert (
        date VARCHAR,
        time VARCHAR,
        charge_point_id VARCHAR,
        charge_point_type VARCHAR,
        status VARCHAR,
        coordinates VARCHAR,
        address VARCHAR,
        longitude float,
        latitude float,
        datetime TIMESTAMP
    );''')
    conn.commit()
    connectionPool.putconn(conn)


def getAllFiles():
    conn = connectionPool.getconn()
    cursor = conn.cursor()
    cursor.execute('SELECT name from file')
    conn.commit()
    names = [r[0] for r in cursor.fetchall()]
    connectionPool.putconn(conn)
    return names


def insertFile(file):
    conn = connectionPool.getconn()
    cursor = conn.cursor()
    insertFile = "INSERT INTO file (name) VALUES (%s)"
    cursor.execute(insertFile, [file])
    conn.commit()
    connectionPool.putconn(conn)


def bulkInsert(data, columns):
    conn = connectionPool.getconn()
    cursor = conn.cursor()
    cursor.copy_from(data, "bulk_insert", columns=tuple(columns), sep='|')
    conn.commit()
    connectionPool.putconn(conn)


def moveDataFromTemporaryTable():
    conn = connectionPool.getconn()
    sql = '''INSERT INTO charge_point (charge_point_id, latitude, longitude, address, type)
        SELECT charge_point_id,latitude, longitude, address, charge_point_type 
        FROM bulk_insert
    ON CONFLICT Do NOTHING;
    
    INSERT INTO register_date (date)
        SELECT distinct datetime 
        FROM bulk_insert
    ON CONFLICT Do NOTHING;
    
    INSERT INTO register_status (status, fk_charge_point, fk_register_date) SELECT status, (select id from 
    charge_point as cp where bi.latitude = cp.latitude and bi.longitude = cp.longitude and bi.charge_point_type = 
    cp.type), (select id from register_date  as rd where rd.date = datetime) FROM bulk_insert as bi ON CONFLICT Do 
    NOTHING; '''
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    connectionPool.putconn(conn)


def truncateBulkInsertTable():
    conn = connectionPool.getconn()
    sql = 'TRUNCATE TABLE bulk_insert;'
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    connectionPool.putconn(conn)


def getChargePoints(start_date, end_date, stat_time, end_time):
    conn = connectionPool.getconn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    selectQueryStart = '''
    SELECT cp.*, rd.* from charge_point cp cross join lateral
        (select
            COUNT(rs.status) filter (where rs.status = 'Occ') as total_occ,
            COUNT(rs.status) filter (where rs.status = 'Part') as total_part,
            COUNT(rs.status) filter (where rs.status = 'OOS') as total_oos,
            COUNT(rs.status) filter (where rs.status = 'OOC') as total_ooc,
            rs.fk_charge_point as cpid
        FROM register_date rd 
            inner join register_status rs on rs.fk_register_date = rd.id 
    '''
    selectQueryEnd = ''' group by rs.fk_charge_point) rd
    where cp.id = rd.cpid '''
    whereQuery = mountWhereQuery(start_date, end_date, stat_time, end_time)
    cursor.execute(selectQueryStart+whereQuery+selectQueryEnd)
    conn.commit()
    results = cursor.fetchall()
    connectionPool.putconn(conn)
    return results


def mountWhereQuery(start_date, end_date, start_time, end_time):
    query = ' where 0 = 0 '
    if start_date:
        query += " AND rd.date::date >= '%s'" % start_date
    if end_date:
        query += " AND rd.date::date <= '%s'" % end_date
    if start_time:
        query += " AND rd.date::time >= time '%s'" % start_time
    if end_time:
        query += " AND rd.date::time <= time '%s'" % end_time
    return query


def getMaxMinDate():
    conn = connectionPool.getconn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute('SELECT MAX(date), MIN(date) FROM register_date')
    conn.commit()
    result = cursor.fetchone()
    connectionPool.putconn(conn)
    return result


def getChargeTypes():
    conn = connectionPool.getconn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute('SELECT DISTINCT type FROM charge_point')
    conn.commit()
    results = cursor.fetchall()
    connectionPool.putconn(conn)
    return results
# Performance of this alternative method was not great
#
# def insertChargePoint(charge_point):
#     global conn
#     cursor = conn.cursor()
#     insertSql = '''INSERT INTO charge_point (charge_point_id, latitude, longitude, address, type)
#         VALUES (%s, %s, %s, %s, %s)
#         ON CONFLICT (latitude, longitude) DO UPDATE SET type=EXCLUDED.type, charge_point_id=EXCLUDED.charge_point_id
#         RETURNING id'''
#     cursor.execute(insertSql, (charge_point['charge_point_id'], charge_point['latitude'], charge_point['longitude'],
#                                charge_point['address'], charge_point['type']))
#     return cursor.fetchone()[0]
#
#
# def insertRegisterData(date):
#     global conn
#     cursor = conn.cursor()
#     insertSql = '''INSERT INTO register_date (date)
#             VALUES (%s)
#             ON CONFLICT (date) DO update set date=EXCLUDED.date
#             RETURNING id'''
#     cursor.execute(insertSql, [date])
#     return cursor.fetchone()[0]
#
#
# def insertRegisterStatus(fk_charge_point, fk_register_date, status):
#     global conn
#     cursor = conn.cursor()
#     insertSql = '''INSERT INTO register_status (fk_charge_point, fk_register_date, status)
#             VALUES (%s, %s, %s)'''
#     cursor.execute(insertSql, (fk_charge_point, fk_register_date, status))
#
#
# def insertEVData(charge_point, date, status):
#     global conn
#     # fkChargePoint = insertChargePoint(charge_point)
#     fkRegisterDate = insertRegisterData(date)
#     # insertRegisterStatus(fkChargePoint, fkRegisterDate, status)
