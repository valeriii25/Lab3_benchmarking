import pandas as pd


class Pandas:
    def __init__(self, path):
        self.taxi = pd.read_csv(path)

    def q1(self):
        selected_df = self.taxi[['VendorID']]
        grouped_df = selected_df.groupby('VendorID')
        final_df = grouped_df.size().reset_index(name='counts')
        return final_df

    def q2(self):
        selected_df = self.taxi[['passenger_count', 'total_amount']]
        grouped_df = selected_df.groupby('passenger_count')
        final_df = grouped_df.mean().reset_index()
        return final_df

    def q3(self):
        selected_df = self.taxi[['passenger_count', 'tpep_pickup_datetime']]
        selected_df['year'] = pd.to_datetime(
            selected_df.pop('tpep_pickup_datetime'),
            format='%Y-%m-%d %H:%M:%S').dt.year
        grouped_df = selected_df.groupby(['passenger_count', 'year'])
        final_df = grouped_df.size().reset_index(name='counts')
        return final_df

    def q4(self):
        selected_df = self.taxi[[
            'passenger_count',
            'tpep_pickup_datetime',
            'trip_distance']]
        selected_df['trip_distance'] = selected_df['trip_distance'].round().astype(int)
        selected_df['year'] = pd.to_datetime(
            selected_df.pop('tpep_pickup_datetime'),
            format='%Y-%m-%d %H:%M:%S').dt.year
        grouped_df = selected_df.groupby([
            'passenger_count',
            'year',
            'trip_distance'])
        final_df = grouped_df.size().reset_index(name='counts')
        final_df = final_df.sort_values(
            ['year', 'counts'],
            ascending=[True, False])
        return final_df
