#!/usr/bin/env python
import web, datetime

db_name = 'trackit'
db_app = 'mysql'
db_user = 'trackit'
db_password = 'hobbes87'
db_host = 'localhost'

db = web.database(dbn=db_app, db=db_name, user=db_user, pw=db_password, dburl=db_host)


def setDBConnection(app, name, user=None, password=None):
    db = web.database(dbn=app,db=name,user=user,pw=password)

def select(table,**k):
    return db.select(table,_test=False,**k)

def insert(table,**k):
    return db.insert(table,_test=False,**k)

def delete(table,**k):
    return db.delete(table,_test=False,**k)

def update(table,where,**k):
    db.update(table,where=where,_test=False,**k)
    
def createTable(table,columns,types):
    pass

