'use strict';

var express = require('express');
var app = express();

var moviesRouter = require('./src/routes/moviesRouter');

var port = process.env.PORT || 5000;


//used by express first
app.use(express.static('./public'));
app.use(express.static('./src'));



//templating engine
app.set('views', './src/views');      
app.set('view engine', 'ejs');


app.use('/moviesDetails', moviesRouter.getMoviesDetails());

app.get('/', function (req, res) {
    res.render('index', {
        title: 'Movies Genres Chart'
    });
});

app.get('/wallstreet', function (req, res) {
    res.render('wallstreet', {
        title: 'Wall Street'
    });
});

app.listen(port, function () {
    console.log('running server on port ' + port) 
});
