SELECT title, revenue, release_date FROM MOVIES WHERE release_date >= date('2014-01-01') AND release_date < date('2015-01-01') AND revenue >= 10000000 ORDER BY revenue DESC LIMIT 10;
