""""""
from datetime import datetime
from re import S
from typing import List

import cx_Oracle
from sqlalchemy import Column, Integer, String, DateTime, Float, Sequence, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import (
    BaseDatabase,
    BarOverview,
    DB_TZ,
    convert_tz
)
from vnpy.trader.setting import SETTINGS


Base = declarative_base()


class DbBarData(Base):
    __tablename__ = 'DbBarData'
    id = Column(Integer, Sequence('bar_id_seq'), primary_key=True)

    symbol = Column(String(255))
    exchange = Column(String(255))
    datetime = Column(DateTime)
    interval = Column(String(255))   

    volume = Column(Float)
    open_interest = Column(Float)
    open_price = Column(Float)
    high_price = Column(Float)
    low_price = Column(Float)
    close_price = Column(Float)


class DbTickData(Base):
    __tablename__ = 'DbTickData'
    id = Column(Integer, Sequence('tick_id_seq'),primary_key=True)

    symbol = Column(String(255))
    exchange = Column(String(255))
    datetime = Column(DateTime)

    name = Column(String(255))  
    volume = Column(Float)
    open_interest = Column(Float)
    last_price = Column(Float)
    last_volume = Column(Float)
    limit_up = Column(Float)
    limit_down = Column(Float) 

    open_price: Column(Float)
    high_price: Column(Float)
    low_price: Column(Float)
    pre_close: Column(Float)

    bid_price_1: Column(Float)
    bid_price_2: Column(Float)
    bid_price_3: Column(Float)
    bid_price_4: Column(Float)
    bid_price_5: Column(Float)

    ask_price_1: Column(Float)
    ask_price_2: Column(Float)
    ask_price_3: Column(Float)
    ask_price_4: Column(Float)
    ask_price_5: Column(Float)

    bid_volume_1: Column(Float)
    bid_volume_2: Column(Float)
    bid_volume_3: Column(Float)
    bid_volume_4: Column(Float)
    bid_volume_5: Column(Float)

    ask_volume_1: Column(Float)
    ask_volume_2: Column(Float)
    ask_volume_3: Column(Float)
    ask_volume_4: Column(Float)
    ask_volume_5: Column(Float)


class DbBarOverview(Base):
    __tablename__ = 'DbBarOverview'
    id = Column(Integer, Sequence('overview_id_seq'), primary_key=True)

    symbol = Column(String(255))
    exchange = Column(String(255))
    interval = Column(String(255))   
    count = Column(Integer)
    start = Column(DateTime)
    end = Column(DateTime)


class OracleDatabase(BaseDatabase):
    """Oracle数据库客户端"""

    def __init__(self) -> None:
        """"""
        # 目前只用到host，后面要用URL形式配置
        database = SETTINGS["database.database"]
        user = SETTINGS["database.user"]
        password = SETTINGS["database.password"]
        host = SETTINGS["database.host"]
        port = SETTINGS["database.port"]

        url = f"oracle://{user}:{password}@{host}:{port}/{database}"

        # 连接服务器

        engine = create_engine(url, encoding='utf8', max_identifier_length=128, echo=True)
        Base.metadata.create_all(engine)
        Database = sessionmaker(bind=engine)
        self.db = Database()

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存K线数据"""

        # Store key parameters
        bar = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange
        interval = bar.interval

        # Convert bar object to dict and adjust timezone
        data = []

        for bar in bars:
            data = DbBarData(
                symbol=bar.symbol,
                exchange=exchange.value,
                datetime=convert_tz(bar.datetime),
                interval=interval.value,
                volume=bar.volume,
                open_interest=bar.open_interest,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                close_price=bar.close_price
                )
            self.db.add(data)
        
        self.db.commit()

        overview = self.db.query(DbBarOverview).filter(
            DbBarOverview.symbol == symbol, 
            DbBarOverview.exchange == exchange.value, 
            DbBarOverview.interval == interval.value).first()

        if not overview:
            overview = DbBarOverview(
                symbol=symbol,
                exchange=exchange.value,
                interval=interval.value,
                count=len(bars),
                start=bars[0].datetime,
                end=bars[-1].datetime
                )

        else:
            overview.start = min(bars[0].datetime, overview.start)
            overview.end = max(bars[-1].datetime, overview.end)
            overview.count = self.db.query(DbBarData).filter(
                DbBarData.symbol == symbol,
                DbBarData.exchange == exchange.value,
                DbBarData.interval == interval.value).count()
        
        self.db.add(overview)
        self.db.commit()

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """"""
        # Convert tick object to dict and adjust timezone
        tick = ticks[0]
        exchange = tick.exchange
        data = []

        for tick in ticks:
            data = DbTickData(
                symbol=tick.symbol,
                exchange=exchange.value,
                datetime=convert_tz(tick.datetime),
                volume=tick.volume,
                open_interest=tick.open_interest,
                last_price=tick.last_price,
                last_volume=tick.last_volume,
                limit_up=tick.limit_up,
                limit_down=tick.limit_down,
                open_price=tick.open_price,
                high_price=tick.high_price,
                low_price=tick.low_price,
                pre_close=tick.pre_price,
                bid_price_1=tick.bid_price_1,
                bid_price_2=tick.bid_price_2,
                bid_price_3=tick.bid_price_3,
                bid_price_4=tick.bid_price_4,
                bid_price_5=tick.bid_price_5,
                ask_price_1=tick.ask_price_1,
                ask_price_2=tick.ask_price_2,
                ask_price_3=tick.ask_price_3,
                ask_price_4=tick.ask_price_4,
                ask_price_5=tick.ask_price_5,
                bid_volume_1=tick.bid_volume_1,
                bid_volume_2=tick.bid_volume_2,
                bid_volume_3=tick.bid_volume_3,
                bid_volume_4=tick.bid_volume_4,
                bid_volume_5=tick.bid_volume_5,
                ask_volume_1=tick.ask_volume_1,
                ask_volume_2=tick.ask_volume_2,
                ask_volume_3=tick.ask_volume_3,
                ask_volume_4=tick.ask_volume_4,
                ask_volume_5=tick.ask_volume_5
                )
            self.db.add(data)
        
        self.db.commit()

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """"""

        s = self.db.query(DbBarData).filter(
            DbBarData.symbol == symbol,
            DbBarData.exchange == exchange.value, 
            DbBarData.interval == interval.value,
            DbBarData.datetime >= start,
            DbBarData.datetime <= end
            ).order_by(DbBarData.datetime).all()

        vt_symbol = f"{symbol}.{exchange.value}"
        bars: List[BarData] = []
        for db_bar in s:
            data = BarData(
                symbol=db_bar.symbol,
                exchange=Exchange(db_bar.exchange),
                datetime=DB_TZ.localize(db_bar.datetime),
                interval=Interval(db_bar.interval),
                volume=db_bar.volume,
                open_interest=db_bar.open_interest,
                open_price=db_bar.open_price,
                high_price=db_bar.high_price,
                low_price=db_bar.low_price,
                close_price=db_bar.close_price,
                gateway_name="DB"
                )
            bars.append(data)

        return bars        

    def load_tick_data(
        self,
        symbol: str,
        exchange: Exchange,
        start: datetime,
        end: datetime
    ) -> List[TickData]:
        """加载TICK数据"""
        pass

    def delete_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval
    ) -> int:
        """删除K线数据"""
        
        count = self.db.query(DbBarData).filter(
            DbBarData.symbol == symbol,
            DbBarData.exchange == exchange.value,
            DbBarData.interval == interval.value).count()

        self.db.query(DbBarData).filter(
            DbBarData.symbol == symbol,
            DbBarData.exchange == exchange.value,
            DbBarData.interval == interval.value).delete()

        # Delete bar overview
        self.db.query(DbBarOverview).filter(
            DbBarOverview.symbol == symbol, 
            DbBarOverview.exchange == exchange.value, 
            DbBarOverview.interval == interval.value).delete()

        self.db.commit()
        return count

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        pass

    def get_bar_overview(self) -> List[BarOverview]:
        """查询数据库中的K线整体概况"""
        
        s = self.db.query(DbBarOverview).all()
        overviews = []
        for overview in s:
            data = BarOverview(
                symbol=overview.symbol,
                exchange=Exchange(overview.exchange),
                interval=Interval(overview.interval),
                count = overview.count,
                start=overview.start,
                end=overview.end
                )
            overviews.append(data)
        return overviews


database_manager = OracleDatabase()
