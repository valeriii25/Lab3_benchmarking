from time import perf_counter_ns
from json import loads
import duckdb_funcs
import pandas_funcs
import psycopg2_funcs
import sqlite_funcs
import sqlalchemy_funcs
from psycopg2 import connect


def time(query):
    start = perf_counter_ns()
    query()
    end = perf_counter_ns()
    return end - start


def run(lib_obj, tests):
    av_time = [0] * 4
    for i in range(0, tests):
        av_time[0] += time(lib_obj.q1)
        av_time[1] += time(lib_obj.q2)
        av_time[2] += time(lib_obj.q3)
        av_time[3] += time(lib_obj.q4)
    for i in range(0, 4):
        print(av_time[i] / tests / 1000000000)


def create_tab(path_to_f, p_password):
    conn = connect(f"host='localhost' dbname='postgres' user='postgres' password='{p_password}'")
    cur = conn.cursor()
    cur.execute('''CREATE TABLE taxi
                    (
                        "Id"                  bigint,
                        "VendorID"            bigint,
                        tpep_pickup_datetime  text,
                        tpep_dropoff_datetime text,
                        passenger_count       double precision,
                        trip_distance         double precision,
                        "RatecodeID"          double precision,
                        store_and_fwd_flag    text,
                        "PULocationID"        bigint,
                        "DOLocationID"        bigint,
                        payment_type          bigint,
                        fare_amount           double precision,
                        extra                 double precision,
                        mta_tax               double precision,
                        tip_amount            double precision,
                        tolls_amount          double precision,
                        improvement_surcharge double precision,
                        total_amount          double precision,
                        congestion_surcharge  double precision,
                        airport_fee           double precision
                    );
                ''')
    import_csv_sql = '''COPY taxi ("Id", "VendorID", tpep_pickup_datetime,
                             tpep_dropoff_datetime, passenger_count, trip_distance,
                             "RatecodeID", store_and_fwd_flag, "PULocationID",
                             "DOLocationID", payment_type, fare_amount, extra,
                             mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                             total_amount, congestion_surcharge, airport_fee)
                        FROM STDIN
                        DELIMITER ',' CSV HEADER;'''
    with open(path_to_f) as csv_file:
        cur.copy_expert(import_csv_sql, csv_file)
    conn.commit()
    cur.close()
    conn.close()


def delete_tab(p_password):
    conn = connect(f"host='localhost' dbname='postgres' user='postgres' password='{p_password}'")
    cur = conn.cursor()
    cur.execute('''DROP TABLE if exists taxi;''')
    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    file = open('config.json')
    config = loads(file.read())
    tests_count = config['tests_count']
    path = config['path_to_file']
    password = config['postgres_password']
    try:
        create_tab(path, password)
        if config['pandas']:
            print("Pandas:")
            run(pandas_funcs.Pandas(path), tests_count)
        if config['psycopg2']:
            print("Psycopg2:")
            run(psycopg2_funcs.Psycopg2(password), tests_count)
        if config['sqlite']:
            print("SQLite:")
            run(sqlite_funcs.SQLite(path), tests_count)
        if config['duckdb']:
            print("DuckDB:")
            run(duckdb_funcs.DuckDB(path), tests_count)
        if config['sqlalchemy']:
            print("SQLAlchemy:")
            run(sqlalchemy_funcs.SQLAlchemy(password), tests_count)
    finally:
        delete_tab(password)
