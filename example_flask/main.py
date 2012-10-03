#!/usr/bin/env python
# -*- coding:utf-8 -*-

from flask import Flask

from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

app = Flask(__name__)

engine = create_engine('sqlite:////tmp/text.db', convert_unicode=True)
db_session = scoped_session(
    sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()
Base.query = db_session.query_property()

class Movie(Base):
    __tablename__ = 'movie'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))

def init_db():
    Base.metadata.create_all(bind=engine)

@app.teardown_request
def shutdown_session(exception=None):
    db_session.remove()

@app.route('/')
def index():
    return u'Hello world'

@app.route('/test')
def test_db():
    return unicode(Movie.query.count())

if __name__ == '__main__':
    init_db()

    app.run()
