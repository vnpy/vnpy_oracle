""""""
from datetime import datetime
from typing import List

import cx_Oracle
from sqlalchemy import Column, Integer, String, DateTime, Float, Sequence, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.object import BarData, TickData
from vnpy.trader.database import BaseDatabase, BarOverview
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

        database = "XE"
        user = "SYSTEM"
        password = "vnpy"
        host = "localhost"
        port = "1521"

        url = f"oracle://{user}:{password}@{host}:{port}/{database}"

        # 连接服务器

        engine = create_engine(url, encoding='utf8', max_identifier_length=128)
        Base.metadata.create_all(engine)
        Database = sessionmaker(bind=engine)
        self.db = Database()

    def save_bar_data(self, bars: List[BarData]) -> bool:
        """保存K线数据"""
        pass

    def save_tick_data(self, ticks: List[TickData]) -> bool:
        """保存TICK数据"""
        pass

    def load_bar_data(
        self,
        symbol: str,
        exchange: Exchange,
        interval: Interval,
        start: datetime,
        end: datetime
    ) -> List[BarData]:
        """加载K线数据"""
        pass

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
        pass

    def delete_tick_data(
        self,
        symbol: str,
        exchange: Exchange
    ) -> int:
        """删除TICK数据"""
        pass

    def get_bar_overview(self) -> List[BarOverview]:
        """查询数据库中的K线整体概况"""
        pass


database_manager = OracleDatabase()
