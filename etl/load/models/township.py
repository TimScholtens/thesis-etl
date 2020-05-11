from sqlalchemy import Column, Integer, String
from config import SQLALCHEMY_BASE


class Township(SQLALCHEMY_BASE):
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    address = Column(String)
    email = Column(String)


