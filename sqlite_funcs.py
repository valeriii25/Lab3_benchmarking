import sqlite3 as sl
from pandas import read_csv


class SQLite:
    def __init__(self, path):
        self.conn = sl.connect(":memory:")
        read_csv(path).to_sql('taxi', self.conn, if_exists='replace', index=False)
        self.cur = self.conn.cursor()

    def q1(self):
        self.cur.execute('SELECT "VendorID", count(*) FROM taxi GROUP BY 1;')
        return self.cur.fetchall()

    def q2(self):
        self.cur.execute('SELECT passenger_count, avg(total_amount) FROM taxi GROUP BY 1;')
        return self.cur.fetchall()

    def q3(self):
        self.cur.execute(
            '''
                SELECT 
                    passenger_count, 
                    strftime('%Y', tpep_pickup_datetime) AS "Year", 
                    count(*) 
                FROM taxi 
                GROUP BY 1, 2;
            '''
        )
        return self.cur.fetchall()

    def q4(self):
        self.cur.execute(
            '''
                SELECT 
                    passenger_count,
                    strftime('%Y', tpep_pickup_datetime) AS "Year", 
                    round(trip_distance), 
                    count(*) 
                FROM taxi 
                GROUP BY 1, 2, 3 
                ORDER BY 2, 4 desc;
            '''
        )
        return self.cur.fetchall()

    def __del__(self):
        self.cur.close()
        self.conn.close()
