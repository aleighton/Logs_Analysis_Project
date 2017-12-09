import psycopg2

DBNAME = 'news'

def get_top_articles():
    QUERY = """SELECT articles.title, count(*) AS views
            FROM articles JOIN log
            ON log.path LIKE CONCAT('%',articles.slug,'%')
            WHERE log.status = '200 OK' and NOT log.path='/'
            GROUP BY articles.title ORDER BY views desc LIMIT 3;"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(QUERY)
    row = c.fetchone()
    print("   Article Title   |   Views   ")
    while row is not None:
        print "   %s   |   %d   " % row
        row = c.fetchone()
    db.close()
    print "\n"
def get_top_authors():
    QUERY = """SELECT authors.name, SUM(article_views.views) AS views
            FROM authors JOIN article_views
            ON authors.id = article_views.author
            GROUP BY authors.name ORDER BY views desc;"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(QUERY)
    row = c.fetchone()
    print("   Author   |   Article Views   ")
    while row is not None:
        print "   %s   |   %d   " % row
        row = c.fetchone()
    db.close()
    print "\n"
def get_request_errors():
    QUERY = """SELECT percentages.time, percentages."% of bad requests"
            FROM (SELECT bad_requests.time,
            CAST(bad_requests.bad_requests AS FLOAT)/total_requests.total_requests*100.0
            AS "% of bad requests"
            FROM bad_requests JOIN total_requests
            ON bad_requests.time = total_requests.time) percentages
            WHERE percentages."% of bad requests" > 1.0;"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute(QUERY)
    row = c.fetchone()
    print("   Date   |   Bad Requests above 1%   ")
    while row is not None:
        print "   %s   |   %2f   " % row
        row = c.fetchone()
    db.close()
    print "\n"
get_top_articles()
get_top_authors()
get_request_errors()
