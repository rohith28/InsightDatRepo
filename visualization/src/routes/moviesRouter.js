var cors = require('cors'),
express = require('express'),
moviesRouter = express.Router(),        
mysql = require('mysql');

var connection = mysql.createConnection({
 connectionLimit : 1, //important 
 host :'mysqlinstance.cjm8qag6rwgx.us-east-1.rds.amazonaws.com',
 port : '3306',
 user: 'mydb',
 password : '9542582841',
 database : 'moviebuff'
 //host     : process.env.RDS_HOSTNAME,
 //user     : process.env.RDS_USERNAME,
 //password : process.env.RDS_PASSWORD,
 //port     : process.env.RDS_PORT,
 //database : process.env.RDS_DB_NAME
});

connection.connect(function(err) {
    if (err) throw err;
    console.log("Connected!");
  });
  
moviesRouter.all('*', cors());

var getMoviesDetails = function(){
    
    moviesRouter.route('/')    
    .get(function(req,res){
        //connection.query("SELECT year(releaseDate) as year,sum(revenue - budget) as profit  FROM movies WHERE genres LIKE '%Comedy%' AND releaseDate < '2018-09-20' AND budget>'0' AND revenue > '0' GROUP BY year(releaseDate) ORDER BY releaseDate", function(err, rows, fields) {
       /*connection.query("select C.year, C.comdey_profit as comedy, A.action_profit as action, Cr.crime_profit as crime FROM ( SELECT year(releaseDate) year, sum(IFNULL(revenue, '0') - IFNULL(budget, '0')) as comdey_profit  FROM movies WHERE genres LIKE '%Comedy%'  AND releaseDate < '2018-09-20'"+
        "AND year(releaseDate) > '1983'"+
        "AND budget >'0' AND revenue > '0' GROUP BY year(releaseDate) ORDER BY year(releaseDate)) C"+
        "left join ( SELECT year(releaseDate) year, sum(IFNULL(revenue, '0') - IFNULL(budget, '0')) as action_profit FROM movies"+
        "WHERE genres LIKE '%Action%' AND releaseDate < '2018-09-20' AND budget >'0' AND revenue > '0' "+
        "GROUP BY year(releaseDate)"+
        "ORDER BY year(releaseDate)) A ON C.year = A.year"+
        "LEFT JOIN ("+
        "SELECT year(releaseDate) year, sum(IFNULL(revenue, '0') - IFNULL(budget, '0')) as crime_profit"+ 
        "FROM movies WHERE genres LIKE '%Crime%' AND releaseDate < '2018-09-20' AND budget>'0'  AND revenue > '0' GROUP BY year(releaseDate) ORDER BY year(releaseDate)) Cr ON C.year = Cr.year"+
        "order by year(releaseDate) ", function(err, rows, fields) {*/

        connection.query("select C.year, C.comdey_profit as comedy, A.action_profit as action, Cr.crime_profit as crime FROM ( SELECT year(releaseDate) year, sum(revenue- budget) as comdey_profit  FROM movies WHERE genres LIKE '%Comedy%'  AND releaseDate < '2018-09-20' AND year(releaseDate) > '1983' AND budget >'0' AND revenue > '0' GROUP BY year(releaseDate) ORDER BY year(releaseDate)) C left join ( SELECT year(releaseDate) year, sum(revenue - budget) as action_profit FROM movies WHERE genres LIKE '%Action%' AND releaseDate < '2018-09-20' AND budget >'0' AND revenue > '0' GROUP BY year(releaseDate) ORDER BY year(releaseDate)) A ON C.year = A.year LEFT JOIN ( SELECT year(releaseDate) year, sum(revenue - budget) as crime_profit FROM movies WHERE genres LIKE '%Crime%' AND releaseDate < '2018-09-20' AND budget>'0'  AND revenue > '0' GROUP BY year(releaseDate) ORDER BY year(releaseDate)) Cr ON C.year = Cr.year ", function(err, rows, fields) {
    
        //connection.query("SELECT year(releaseDate) as year, budget, revenue, revenue-budget as money      FROM movies  WHERE genres LIKE '%Adventure%' AND year(releaseDate) > '1981' AND year(releaseDate) < '2019' AND budget > '0' AND revenue > '0' AND revenue-budget > '0' ORDER BY year(releaseDate)", function(err, rows, fields) {
           
            if (err) {
                console.error(err);
                res.statusCode = 500;
                res.send({
                    result: 'error',
                    err:    err.code
                });
            }
           
            res.send(rows);
        }); 
    });
    return moviesRouter;
        
};


module.exports = {
    getMoviesDetails: getMoviesDetails
};
