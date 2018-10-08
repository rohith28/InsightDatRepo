function myFunction(){
    myGenres();
    myProfits();   
    myProd(); 
    jQuery.scrollSpeed(100, 400); // for smooth mouse wheel scrolling   
}
    
function myGenres(){
    console.log("We are here");
    var val = document.getElementById('myRadio');
    var year = val.options[val.selectedIndex].text;
    var nowYear = year;
    //alert(user);
    console.log("value is"+year);
    console.log("In genres");
    dataVal = new Array();
    //console.log("2"+val);
    
    $.getJSON("http://movietrends-env.ihde3ha3pe.us-east-1.elasticbeanstalk.com/genres",{
        year: year 
    },function(data){  
        $.each(data, function(key, value){
            console.log("In the genres"+value["score"])
                dataVal.push({ y: parseInt(value["score"]), label: value["name"]});
            });
           genreChart.render();
           console.log(dataVal)   
        });


    var genreChart = new CanvasJS.Chart("popularityChart", {
        animationEnabled: true,
        title: {
            text: "Popularity based on Genre"
        },
        axisX:{
            interval: 1
        },
        axisY2:{
            interlacedColor: "rgba(1,77,101,.2)",
            gridColor: "rgba(1,77,101,.1)",
            title: "Popularity"
        },
        data: [{
            type: "bar",
            name: "companies",
            axisYType: "secondary",
            color: "#014D65",
            dataPoints: dataVal
        }]
    });

}


function myProfits(){

    dataBudget = new Array();
    dataRevenue = new Array();
    dataProfit = new Array();
    var val = document.getElementById('myRadio');
    var year = val.options[val.selectedIndex].text;
    var nowYear = year;

    console.log("Before the profit ")
    $.getJSON("http://movietrends-env.ihde3ha3pe.us-east-1.elasticbeanstalk.com/profit",{
        nowYear: nowYear 
    },function(data){  
        $.each(data, function(key, value){
           console.log("Year:   "+nowYear+" month   "+value["month"]+"  Budget  "+value["budget"]+"   revenue   "+value["revenue"]+"  profit  "+value["profit"]);
            dataBudget.push({ x: new Date(year,value["month"]-1), y: value["budget"]});
            dataRevenue.push({ x: new Date(year,value["month"]-1), y: value["revenue"]});
            dataProfit.push({ x: new Date(year,value["month"]-1), y: value["profit"]});
            });
           
           chartProfit.render(); 
           console.log(dataVal)   
        });
        
    var chartProfit = new CanvasJS.Chart("revenueChart", {
        animationEnabled: true,
        theme: "light2",
        title: {
            text: "Monthly Movies Revenue Data"
        },
        axisX: {
            valueFormatString: "MMM"
        },
        axisY: {
            prefix: "$",
            labelFormatter: addSymbols
        },
        toolTip: {
            shared: true
        },
        legend: {
            cursor: "pointer",
            itemclick: toggleDataSeries
        },
        data: [
        {
            type: "column",
            name: "Profit",
            showInLegend: true,
            xValueFormatString: "MMMM YYYY",
            yValueFormatString: "$#,##0",
            dataPoints: dataProfit
        }, 
        {
            type: "line",
            name: "Budget",
            showInLegend: true,
            yValueFormatString: "$#,##0",
            dataPoints: dataBudget
        },
        {
            type: "line",
            name: "Revenue",
            markerBorderColor: "white",
            markerBorderThickness: 2,
            showInLegend: true,
            yValueFormatString: "$#,##0",
            dataPoints: dataRevenue
        }]
    });


    function addSymbols(e) {
        var suffixes = ["", "K", "M", "B"];
        var order = Math.max(Math.floor(Math.log(e.value) / Math.log(1000)), 0);
    
        if(order > suffixes.length - 1)                	
            order = suffixes.length - 1;
    
        var suffix = suffixes[order];      
        return CanvasJS.formatNumber(e.value / Math.pow(1000, order)) + suffix;
    }
    
    function toggleDataSeries(e) {
        if (typeof (e.dataSeries.visible) === "undefined" || e.dataSeries.visible) {
            e.dataSeries.visible = false;
        } else {
            e.dataSeries.visible = true;
        }
        e.chart.render();
    }


}
    
function myProd(){
    console.log("We are here in prod");
    var val = document.getElementById('myRadio');
    var year = val.options[val.selectedIndex].text;
    var nowYear = year;
    //alert(user);
    console.log("value is"+year);
    console.log("In genres");
    dataValProd = new Array();
    //console.log("2"+val);
    
    $.getJSON("http://movietrends-env.ihde3ha3pe.us-east-1.elasticbeanstalk.com/prod",{
        nowYear: nowYear 
    },function(data){  
        $.each(data, function(key, value){
            console.log("In the genres"+value["score"])
                dataValProd.push({ y: value["percent"], label: value["name"]});
            });
            prodChart.render();
           console.log(dataValProd)   
        });

        var textProd = "Top 10 Production budget percentage in Market  "+nowYear;
        var prodChart = new CanvasJS.Chart("productionChart", {
            animationEnabled: true,
            title: {
                text: textProd
            },
            data: [{
                type: "pie",
                startAngle: 240,
                yValueFormatString: "##0.00\"%\"",
                indexLabel: "{label} {y}",
                dataPoints: dataValProd
            }]
        });
        

}
