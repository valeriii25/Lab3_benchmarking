import duckdb as db


class DuckDB:
    def __init__(self, path):
        db.sql(f'''CREATE TABLE taxi AS FROM '{path}';''')

    def q1(self):
        db.sql('SELECT "VendorID", count(*) FROM taxi GROUP BY 1;')

    def q2(self):
        db.sql('SELECT passenger_count, avg(total_amount) FROM taxi GROUP BY 1;')

    def q3(self):
        db.sql('''
                SELECT 
                    passenger_count, 
                    date_part('Year', tpep_pickup_datetime::date), 
                    count(*) 
                FROM taxi 
                GROUP BY 1, 2;
            ''')

    def q4(self):
        db.sql('''
                SELECT 
                    passenger_count,
                    date_part('Year', tpep_pickup_datetime::date), 
                    round(trip_distance), 
                    count(*) 
                FROM taxi 
                GROUP BY 1, 2, 3 
                ORDER BY 2, 4 desc;
            ''')
