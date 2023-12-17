import psycopg2 as ps


class Psycopg2:
    def __init__(self, password):
        self.conn = ps.connect(f"host='localhost' dbname='postgres' user='postgres' password='{password}'")
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
                    date_part('Year', tpep_pickup_datetime::date), 
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
                    date_part('Year', tpep_pickup_datetime::date), 
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
