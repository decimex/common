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

    class Link(base):
        __tablename__ = 'links'
        url = Column(String, primary_key=True, unique=True, nullable=False)
        is_parsed = Column(Boolean, unique=False, server_default='false', default=False)
        time_created = Column(DateTime(timezone=True), server_default=func.now())

    def initialize(self):
        db_string = "{0}://{1}:{2}@{3}:{4}/{5}".format(db_type, user_name, password, host, port, db_name)
        self.db = create_engine(db_string)

    def start_session(self):
        Session = sessionmaker(self.db)
        self.session = Session()

    def create_defined_tables(self):
        self.start_session()
        self.base.metadata.create_all(self.db)

    def add_link(self, url):
        self.statement = self.Link(url=url)
        self.session.add(self.statement)

    def get_link(self, url=None, is_parsed=None):
        self.statement = self.Link
        if url is not None and is_parsed is None:
            link = self.session. \
            query(self.statement). \
            filter(self.statement.url == url). \
            all()
        elif is_parsed is not None and url is None:
            link = self.session. \
            query(self.statement). \
            filter(self.statement.is_parsed == is_parsed). \
            all()
        elif url is not None and is_parsed is not None:
            link = self.session. \
            query(self.statement). \
            filter(self.statement.is_parsed == is_parsed). \
            filter(self.statement.url == url). \
            all()
        else:
            link = None
        return link

    def update_link(self, url, is_parsed):
        self.session. \
        query(self.statement). \
        filter(self.statement.url == url). \
        update({"is_parsed": is_parsed})

    def delete_link(self, url=None, is_parsed=None):
        self.statement = self.Link
        if url is not None and is_parsed is None:
            self.session. \
            query(self.statement). \
            filter(self.statement.url == url). \
            delete()
        elif is_parsed is not None and url is None:
            self.session. \
            query(self.statement). \
            filter(self.statement.is_parsed == is_parsed). \
            delete()
        elif url is not None and is_parsed is not None:
            self.session. \
            query(self.statement). \
            filter(self.statement.is_parsed == is_parsed). \
            filter(self.statement.url == url). \
            delete()

    def commit_session(self):
        self.session.commit()


db_type     = "postgres"
user_name   = "finder"
password    = "Aa123456"
host        = "localhost"
port        = "5432"
db_name     = "issues"

db = IssuesDb(db_type, user_name, password, host, port, db_name)
db.initialize()
db.create_defined_tables()
# db.add_link("blabla")
# db.add_link("gaga")
db.get_link(url="gaga")
db.update_link(url="gaga", is_parsed=False)
db.delete_link(is_parsed=False)
db.commit_session()

results = db.get_link(url="gaga")
for result in results:
    print(result.is_parsed)
