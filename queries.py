#!/usr/bin/env python3
import psycopg2

DBNAME = 'news'


def get_top_articles():
    QUERY = """SELECT articles.title, count(*) AS views
            FROM articles JOIN log
            ON log.path LIKE CONCAT('%',articles.slug,'%')
            WHERE log.status = '200 OK' and NOT log.path='/'
            GROUP BY articles.title ORDER BY views desc LIMIT 3;"""

    def connect(database_name="news"):
        try:
            db = psycopg2.connect("dbname={}".format(database_name))
            cursor = db.cursor()
        except(RuntimeError):
            print("Database was unable to be opened.")
        else:
            cursor.execute(QUERY)
            row = cursor.fetchone()
            print("   Article Title   |   Views   ")
            while row is not None:
                print"   %s   |   %d   " % row
                row = cursor.fetchone()
            db.close()
            print "\n"
    connect()


def get_top_authors():
    QUERY = """SELECT authors.name, SUM(article_views.views) AS views
            FROM authors JOIN article_views
            ON authors.id = article_views.author
            GROUP BY authors.name ORDER BY views desc;"""

    def connect(database_name="news"):
        try:
            db = psycopg2.connect("dbname={}".format(database_name))
            cursor = db.cursor()
        except(RuntimeError):
            print("Database was unable to be opened.")
        else:
            cursor.execute(QUERY)
            row = cursor.fetchone()
            print("   Author   |   Article Views   ")
            while row is not None:
                print "   %s   |   %d   " % row
                row = cursor.fetchone()
            db.close()
            print "\n"
    connect()


def get_request_errors():
    QUERY = """SELECT percentages.time, percentages."% of bad requests"
            FROM (SELECT bad_requests.time,
            CAST(bad_requests.bad_requests AS FLOAT)
            /total_requests.total_requests*100.0
            AS "% of bad requests"
            FROM bad_requests JOIN total_requests
            ON bad_requests.time = total_requests.time) percentages
            WHERE percentages."% of bad requests" > 1.0;"""

    def connect(database_name="news"):
        try:
            db = psycopg2.connect("dbname={}".format(database_name))
            cursor = db.cursor()
        except(RuntimeError):
            print("Database was unable to be opened.")
        else:
            cursor.execute(QUERY)
            row = cursor.fetchone()
            print("   Date   |   Bad Requests above 1%   ")
            while row is not None:
                print "   %s   |   %.2f   " % row
                row = cursor.fetchone()
            db.close()
            print "\n"
    connect()


get_top_articles()
get_top_authors()
get_request_errors()
