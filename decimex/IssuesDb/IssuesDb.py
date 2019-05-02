from sqlalchemy import create_engine
from sqlalchemy import Column, String, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
import datetime

class IssuesDb():
    def __init__(self, db_type, user_name, password, host, port, db_name):
        self.db_type   = db_type
        self.user_name = user_name
        self.password  = password
        self.host      = host
        self.port      = port
        self.db_name   = db_name
        self.initialize()

    base = declarative_base()

    def initialize(self):
        db_string = "{0}://{1}:{2}@{3}:{4}/{5}".format(self.db_type, self.user_name, self.password, self.host, self.port, self.db_name)
        self.db = create_engine(db_string)
        self.Session = sessionmaker(self.db)

    def get_session(self):
        session = self.Session()
        return WrappedSession(session)

    def create_defined_tables(self):
        self.base.metadata.create_all(self.db)

    def add_link(self, session, url):
        link = Link(url=url)
        session.add(link)

    def get_link(self, session, url=None, is_parsed=None):
        query = session.query(Link)
        if url is not None:
            query = query.filter(Link.url == url)
        if is_parsed is not None:
            query = query.filter(Link.is_parsed == is_parsed)
        if url is None and is_parsed is None:
            return None
        return query.all()

    def get_first_link(self, session):
        query = session.query(Link.is_parsed == False).first()
        return query.all()

    def update_link_is_parsed(self, session, url, is_parsed):
        session. \
        query(Link). \
        filter(Link.url == url). \
        update({"is_parsed": is_parsed})

    def delete_link(self, session, url=None, is_parsed=None):
        query = session.query(Link)
        if url is not None:
            query = query.filter(Link.url == url)
        if is_parsed is not None:
            query = query.filter(Link.is_parsed == is_parsed)
        if url is None and is_parsed is None:
            return None
        return query.delete()

    def commit_session(self, session):
        session.commit()

class WrappedSession(object):
    def __init__(self, session):
        self.session = session

    def __getattr__(self, key):
        return getattr(self.session, key)

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.rollback()

class Link(IssuesDb.base):
    __tablename__ = 'links'
    url = Column(String, primary_key=True, unique=True, nullable=False)
    is_parsed = Column(Boolean, unique=False, server_default='false', default=False)
    time_created = Column(DateTime(timezone=True), server_default=func.now())
