var cors = require('cors'),
express = require('express'),
genresRouter = express.Router(),        
mysql = require('mysql');

var connection = mysql.createConnection({
 connectionLimit : 1, //important 
 host : process.env.HOST_NAME,
 port : process.env.PORT,
 user: process.env.USER,
 password : process.env.PWD,
 database : process.env.DATABASE
});

connection.connect(function(err) {
    if (err) throw err;
    console.log("Connected!");
  });
  
  genresRouter.all('*', cors());

var getGenres = function(){
    
    genresRouter.route('/')    
    .get(function(req,res){
       
        year = req.query.year;
        console.log("duplicate in genres"+year);

        connection.query("SELECT avg(popularity) as score, genreName as name from movies m LEFT JOIN genres g ON  g.movie_id = m.movie_id WHERE year(releasedate)=? AND genreName IS NOT NULL  AND popularity>0 GROUP BY genreName ORDER BY score DESC LIMIT 10 ",[year], function(err, rows, fields) {
           
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
    return genresRouter;
        
};



module.exports = {
  getGenres : getGenres
};
