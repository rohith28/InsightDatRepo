var cors = require('cors'),
express = require('express'),
productionRoute = express.Router(),        
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
  
  productionRoute.all('*', cors());



var getProduction = function(){
    
    productionRoute.route('/')    
    .get(function(req,res){
       
        year = req.query.nowYear;
        console.log("duplicate in production"+ year);
        
        connection.query("SELECT sum(budget) ,pName as name, (sum(budget)/(SELECT sum(budget) FROM movies m LEFT JOIN production p ON p.movie_id=m.movie_id WHERE year(releasedate)=?))*100  as percent FROM movies m  LEFT JOIN production p ON p.movie_id = m.movie_id WHERE year(releasedate) = ? AND budget > 0 GROUP BY pName ORDER BY sum(budget) DESC LIMIT 10",[year,year], function(err, rows, fields) {
           
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
    return productionRoute;
        
};

module.exports = {

    getProduction : getProduction
   };
   