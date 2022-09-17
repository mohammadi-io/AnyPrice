from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String


Base = declarative_base()


class Example(Base):
    __tablename__ = 'example'
    id = Column(Integer, primary_key=True)
    column_a = Column(String)
    column_b = Column(String)
