var cors = require('cors'),
express = require('express'),
profitRouter = express.Router(),        
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
  
profitRouter.all('*', cors());



var getProfits = function(){
    
    profitRouter.route('/')    
    .get(function(req,res){
       
        year = req.query.nowYear;
        console.log("duplicate in profit"+year);
        
        connection.query("SELECT round(avg(budget)) as budget,round(avg(revenue)) as revenue,round(avg(revenue-budget)) as profit, month(releasedate) as month from movies WHERE year(releasedate) = ? AND revenue>0 AND budget>0 GROUP BY month(releasedate)",[year], function(err, rows, fields) {
           
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
    return profitRouter;
        
};

module.exports = {

     getProfits : getProfits
   };
   