Listed below are the VIEWs required for the LOGS ANALYSIS PROJECT

1.Creates an aggregated table of total views for each article

CREATE VIEW article_views
AS SELECT articles.author, articles.title, count(*) AS views
FROM articles JOIN log
ON log.path LIKE CONCAT('%',articles.slug,'%')
WHERE log.status = '200 OK' AND NOT log.path='/'
GROUP BY articles.author, articles.title ORDER BY views desc;


CREATE VIEW total_requests
AS SELECT requests.time, SUM(requests.request) AS total_requests
FROM (SELECT time::date, COUNT(*) AS request FROM log GROUP BY time) requests
GROUP BY time ORDER BY time asc;

CREATE VIEW bad_requests
AS SELECT requests.time, SUM(requests.request) AS bad_requests
FROM (SELECT time::date, COUNT(*) as request from log
WHERE NOT status='200 OK' GROUP BY time) requests
GROUP BY time ORDER BY time asc;
select bad_requests.time, (CAST(bad_requests.bad_requests as FLOAT)/total_requests.total_requests*100.0) as "% of bad requests" from bad_requests JOIN total_requests ON bad_requests.time = total_requests.time GROUP BY bad_requests.time, bad_requests."% of bad_requests";
