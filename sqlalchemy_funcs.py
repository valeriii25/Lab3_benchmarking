import sqlalchemy as sa
from sqlalchemy.orm import Mapped, DeclarativeBase, mapped_column, Session
from sqlalchemy.types import TIMESTAMP


class Base(DeclarativeBase):
    pass


class Taxi(Base):
    __tablename__ = 'taxi'

    id: Mapped[int] = mapped_column(primary_key=True)
    VendorID: Mapped[int]
    tpep_pickup_datetime: Mapped[str]
    tpep_dropoff_datetime: Mapped[str]
    passenger_count: Mapped[float]
    trip_distance: Mapped[float]
    RatecodeID: Mapped[int]
    store_and_fwd_flag: Mapped[str]
    PULocationID: Mapped[int]
    DOLocationID: Mapped[int]
    payment_type: Mapped[int]
    fare_amount: Mapped[float]
    extra: Mapped[float]
    mta_tax: Mapped[float]
    tip_amount: Mapped[float]
    tolls_amount: Mapped[float]
    improvement_surcharge: Mapped[float]
    total_amount: Mapped[float]
    congestion_surcharge: Mapped[float]
    airport_fee: Mapped[float]


class SQLAlchemy:
    def __init__(self, password):
        engine = sa.create_engine(f"postgresql+psycopg2://postgres:{password}@localhost:5432/postgres")
        self.session = Session(engine)

    def q1(self):
        return (self.session
                .query(Taxi.VendorID, sa.func.count(Taxi.VendorID))
                .group_by(Taxi.VendorID).all()
                )

    def q2(self):
        return (self.session
                .query(Taxi.passenger_count, sa.func.avg(Taxi.total_amount))
                .group_by(Taxi.passenger_count)
                .all()
                )

    def q3(self):
        return (self.session
                .query(
                    Taxi.passenger_count,
                    sa.func.extract('Year', sa.func.cast(Taxi.tpep_pickup_datetime, TIMESTAMP)).label('Year'),
                    sa.func.count()
                      )
                .group_by(Taxi.passenger_count, 'Year')
                .all()
                )

    def q4(self):
        return (self.session
                .query(
                    Taxi.passenger_count,
                    sa.func.extract('Year', sa.func.cast(Taxi.tpep_pickup_datetime, TIMESTAMP)).label('Year'),
                    sa.func.round(Taxi.trip_distance),
                    sa.func.count()
                      )
                .group_by(Taxi.passenger_count, 'Year', Taxi.trip_distance)
                .all()
                )
