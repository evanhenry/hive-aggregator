$(document).ready(function (){

    // define global variable to track presence of changes to parameters since last ajax request
    var paramsChanged = false;
    
    // define global variable to store most recent data
    var currentData;
    
    // Datepicker settings
    $('.input-daterange').datepicker({
        format: "yyyy-mm-dd",
        todayBtn: "linked",
        todayHighlight: true,
        endDate: "today"
    });
    
    // X-axis selector listener (to hide/reveal date picker if "time" is not selected)
    var isDatePickerDisplayed = true;
    $('#x-axis-selector').change(function(){
        
        // if x-axis selector is not set to 'time' and the date picker is displayed
        if (($(this).val() !== "time") && (isDatePickerDisplayed === true)){
            
            // toggle it (hide)
            $("#datepicker").toggle("slide");
            isDatePickerDisplayed = false;
            
        // else if the x-axis selector is set to 'time' and the date picker is not displayed   
        } else if (($(this).val() === "time") && (isDatePickerDisplayed === false)){
            
            // toggle it (reveal)
            $("#datepicker").toggle("slide");
            isDatePickerDisplayed = true;
            
        }
    });
    
    // plot button listener
    $("#plot-btn").click(function (e) {
        event.preventDefault();
        
        // if parameter selections are not up to date
        if (paramsChanged === true) {
            
            // submit ajax request for fresh data
            $.ajax({
                type: "POST",
                url: "graph",
                contentType: 'application/x-www-form-urlencoded; charset=utf-8',
                dataType: 'JSON',
                data: $('form').serialize(), // serialize() wraps up all selected form parameters
                success: function(result) {
                    var json = JSON.parse(result);
                    var numHives = Object.keys(json).length;
                    if (json.hive1.length == 0) {
                        
                        alert("Sorry, there's no data available between " + $("[type='text'][name='start']").val() + " and " + $("[type='text'][name='end']").val());
                        
                    } else {
                        
                        // parse JSON to colummns 
                        var data = parseToColumns(json, numHives);
                        
                        // draw graph (see c3.js documentation for more info)
                        chart.axis.labels({
                            x: $("#x-axis-selector").find(":selected").text(),
                            y: $("#y-axis-selector").find(":selected").text()
                        });
                        chart.load({
                            columns: data
                        });
                        
                        // change state of paramChange bool
                        paramsChanged = false;
                        
                        // save result to currentData global variable
                        currentData = data;
                    };
                },
                error: function(request, errorType, errorMessage) {
                    alert('error: ' + errorType + ' with message ' + errorMessage)
                }
            });
        };
    });
    
    // Export to CSV button listener
    $("#csv-btn").click(function(e){
        event.preventDefault();
        
        // if parameter selections are up to date
        if (paramsChanged === false) {
            
            // convert current data to CSV and initiate download
            var reportName = $('#y-axis-selector').find(':selected').text() + " between " + $("[type='text'][name='start']").val() + " and " + $("[type='text'][name='end']").val();
            JSONToCSVConvertor(currentData, "Hive Data", true);
            
        } else {
            
            // else, request new data (TODO: redundant lines of code should be factored out into discrete function)
            $.ajax({
                type: "POST",
                url: "graph",
                contentType: 'application/x-www-form-urlencoded; charset=utf-8',
                dataType: 'JSON',
                data: $('form').serialize(),
                success: function(result) {
                    var json = JSON.parse(result);
                    var numHives = Object.keys(json).length;
                    if (json.hive1.length == 0) {
                        
                        alert("Sorry, there's no data available between " + $("[type='text'][name='start']").val() + " and " + $("[type='text'][name='end']").val());
                        
                    } else {
                        
                        // parse JSON to colummns 
                        var data = parseToColumns(json, numHives);
                        
                        // draw graph
                        chart.axis.labels({
                            x: $("#x-axis-selector").find(":selected").text(),
                            y: $("#y-axis-selector").find(":selected").text()
                        });
                        chart.load({
                            columns: data
                        });
                        
                        // generate name of report
                        var reportName = $('#y-axis-selector').find(':selected').text() + " between " + $("[type='text'][name='start']").val() + " and " + $("[type='text'][name='end']").val(); 
                        
                        // convert JSON to CSV, initiate download
                        JSONToCSVConvertor(data, reportName, true);
                    
                        // change state of paramChange bool
                        paramsChanged = false;
                        
                        // save result to currentData global variable
                        currentData = data;
                    };
                },
                error: function(request, errorType, errorMessage) {
                    alert('error: ' + errorType + ' with message ' + errorMessage)
                }
            });
        };
    });
    
    
    // Parse json to columns for c3.js
    function parseToColumns(json, numHives){
        
        // determine y-axis parameter
        var param = $("#y-axis-selector").val();
        
        // parse data into columns for c3.js (see http://c3js.org/samples/data_columned.html for more info)
        var data = []; // create empty array to store data (each column will be an array within this one)
        var x = ['x']; // seed first column with 'x' (for x-axis)
        for (var i in json.hive1) {
            var time = json.hive1[i].time; // grab each timestamp from hive1
            x.push(time); // push timestamp to x column     
        };
        data.push(x); // push x column to data array     
        for (i = 1; i <= numHives; i++) {  // create a column for each hive and push them to data array
            var hive = "hive" + i;
            var series = [hive];
            for (var k in json[hive]) { 
                var sample = json[hive][k];
                console.log(param);
                var dataPoint = sample[param];
                series.push(dataPoint);
            }
            data.push(series);
        }
        return data;
    };
    
    // form change listener
    $('#parameter-input').change(function(){
            paramsChanged = true;
    });
    
    // un-focus buttons after click
    $(":button").mouseup(function(){
        $(this).blur();
    });
    
    // generate chart
    var chart = c3.generate({
        data: {
            x: 'x',
            columns : []                       
        },
        axis: {
            x: {
                type: 'category',
                tick: {
                    rotate: -60,
                    multiline: false,
                    culling: {
                        max: 10
                    }
                },
                label: {
                    text: $("#x-axis-selector").find(":selected").text(),
                    position: 'outer-center'
                },  
                height: 130
            },
            y : {
                label: {
                    text: $("#y-axis-selector").find(":selected").text(),
                    position: 'outer-middle'
                }
            }
        },
        point: {
            show: false
        },
        size: {
            height: 450
        },
        zoom: {
            enabled: true
        }
    });
    
    // JSON to CSV converter
    function JSONToCSVConvertor(JSONData, ReportTitle, ShowLabel) {
        
        //If JSONData is not an object then JSON.parse will parse the JSON string in an Object
        var arrData = typeof JSONData != 'object' ? JSON.parse(JSONData) : JSONData;
        
        //Set Report title in first row or line of CSV file
        var CSV = '';    
        
        CSV += ReportTitle + '\r\n\n';
    
        //This condition will generate the Label/Header
        if (ShowLabel) {
            var row = "";
            
            //This loop will extract the label from 1st index of on array
            for (var index in arrData[0]) {
                
                //Now convert each value to string and comma-seprated
                row += index + ',';
            }
    
            row = row.slice(0, -1);
            
            //append Label row with line break
            CSV += row + '\r\n';
        }
        
        //1st loop is to extract each row
        for (var i = 0; i < arrData.length; i++) {
            var row = "";
            
            //2nd loop will extract each column and convert it in string comma-seprated
            for (var index in arrData[i]) {
                row += '"' + arrData[i][index] + '",';
            }
    
            row.slice(0, row.length - 1);
            
            //add a line break after each row
            CSV += row + '\r\n';
        }
    
        if (CSV == '') {        
            alert("Invalid data");
            return;
        }   
        
        //Generate a file name
        var fileName = "Hive Report_";
        //this will remove the blank-spaces from the title and replace it with an underscore
        fileName += ReportTitle.replace(/ /g,"_");   
        
        //Initialize file format you want csv or xls
        var uri = 'data:text/csv;charset=utf-8,' + escape(CSV);
        
        // Now the little tricky part.
        // you can use either>> window.open(uri);
        // but this will not work in some browsers
        // or you will not get the correct file extension    
        
        //this trick will generate a temp <a /> tag
        var link = document.createElement("a");    
        link.href = uri;
        
        //set the visibility hidden so it will not effect on your web-layout
        link.style = "visibility:hidden";
        link.download = fileName + ".csv";
        
        //this part will append the anchor tag and remove it after automatic click
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

});