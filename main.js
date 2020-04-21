// import
const BodyParser = require("body-parser");
const dbConfig = require('./dbConfig');
const domainConfig = require('./domainConfig');
const Express = require("express");
const MongoClient = require("mongodb").MongoClient;
const Timer = require("./timer");
const Queryer = require("./queryer");

// constant variable
const timer = new Timer();
const queryer = new Queryer();
const PORT = 5000;
const grant = "*";

// app
var app = Express();
app.use(BodyParser.json());
app.use(BodyParser.urlencoded({ extended: true }));

// package for log
const fs = require('fs');
const util = require('util');
var log_file = fs.createWriteStream(__dirname + "/log" + "/log.log", {flags : "a"}); // 'a' means appending (old data will be preserved)
var log_stdout = process.stdout;

// function to overwrite the console.log, so as to log the console-context to a text file
console.log = function(d) { //
  log_file.write(util.format(d) + '\n');
  log_stdout.write(util.format(d) + '\n');
};

// SSL cert
const http = require("http");
const https = require("https");
const domainName = domainConfig.DOMAIN;
const cert = fs.readFileSync(__dirname + domainConfig.CERT);
const ca = fs.readFileSync(__dirname + domainConfig.CA);
const key = fs.readFileSync(__dirname + domainConfig.KEY);
const httpsOptions = {
   cert: cert,
   ca: ca,
   key: key
};
const httpsServer = https.createServer(httpsOptions, app);

// kick the server with https
httpsServer.listen(PORT, domainName, () =>{

    MongoClient.connect(dbConfig.CONNECTION_URL, {useUnifiedTopology: true},
                                                {useNewUrlParser: true },
                                                (error, client) => {
        if(error) {

            console.log( timer.getCurrentLocaltimeInIso(true)
                        + ": "
                        + "Fail to Connect DB `"
                        + dbConfig.DATABASE_NAME + "." + dbConfig.COLLECTION
                        + "`.");

            throw error;
        }
        database = client.db(dbConfig.DATABASE_NAME);
        collection = database.collection(dbConfig.COLLECTION);
        console.log( timer.getCurrentLocaltimeInIso(true)
                    + ": "
                    +"Connected to DB `"
                    + dbConfig.DATABASE_NAME + "." + dbConfig.COLLECTION
                    + "`.");
    });
});

// handle: No 'Access-Control-Allow-Origin' - Node / Apache Port Issue
app.use(function (req, res, next) {

    // Website you wish to allow to connect
    res.setHeader('Access-Control-Allow-Origin', grant);
    // Request methods you wish to allow
    res.setHeader('Access-Control-Allow-Methods', 'GET') //other option: POST, OPTIONS, PUT, PATCH, DELETE');
    // Request headers you wish to allow
    res.setHeader('Access-Control-Allow-Headers', 'X-Requested-With,content-type');
    // Set to true if you need the website to include cookies in the requests sent
    // to the API (e.g. in case you use sessions)
    res.setHeader('Access-Control-Allow-Credentials', true);
    // Pass to next layer of middleware
    
    next();
});


app.get("/weather/api", (request, response) =>{

    try{
        let paraRange = request.query.dtRange;
        let dtNow = new Date;

        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                        + ": " 
                        + request.ip 
                        + "; "
                        + "visiting /weather, param={" + paraRange + "}" 
                    );

        switch(paraRange){
            case '1h':
                queryer.get1hWeather(request.ip, response, dtNow);
                break;
            case '12h':
                queryer.get12hWeather(request.ip, response, dtNow);
                break;   
            case '1d':
                queryer.get1dWeather(request.ip, response, dtNow);
                break;   
            case '7d':
                queryer.get7dWeather(request.ip, response, dtNow);
                break;       
            case '1m':
                queryer.get1mWeather(request.ip, response, dtNow);
                break;  
            case '6m':
                queryer.get6mWeather(request.ip, response, dtNow);
                break;                            
            case '1y':
                queryer.get1yWeather(request.ip, response, dtNow);
                break;            
            default:
                console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                            + ": " 
                            + request.ip 
                            + "; "
                            + "Fail; Get /weather, param={" + paraRange + "}; invalid paramter." 
                        );   
                return response.status(400).json({ 
                    status: "error",
                    message: "invalid paramter <br/>  paramter: dtRange = {1h, 12h, 1d, 7d, 1m, 6m, 1y}"
                });

        }
    }
    catch(error){
        let dtNow = new Date;
        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
        + ": " 
        + request.ip 
        + "; "
        + "Error; " + error.toString());
    }
});

