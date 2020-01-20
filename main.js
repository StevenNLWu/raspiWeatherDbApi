const Express = require("express");
const BodyParser = require("body-parser");
const MongoClient = require("mongodb").MongoClient;
const Timer = require("./timer");
const timer = new Timer();
const Queryer = require("./queryer");
const queryer = new Queryer();

var app = Express();
var dbConfig = require('./dbConfig');
app.use(BodyParser.json());
app.use(BodyParser.urlencoded({ extended: true }));


app.listen(5000, () => {
    MongoClient.connect(dbConfig.CONNECTION_URL, {useUnifiedTopology: true},
                                                {useNewUrlParser: true },
                                                (error, client) => {
        if(error) {

            console.log( timer.getCurrentLocaltimeInIso(true)
                        + ": " 
                        +"Fail to Connect DB `" + dbConfig.CONNECTION_URL + "`.");

            throw error;
        }
        database = client.db(dbConfig.DATABASE_NAME);
        collection = database.collection(dbConfig.COLLECTION);
        console.log( timer.getCurrentLocaltimeInIso(true)
                    + ": " 
                    +"Connected to DB `" + dbConfig.CONNECTION_URL + "`.");
    });
});

// handle: No 'Access-Control-Allow-Origin' - Node / Apache Port Issue
app.use(function (req, res, next) {

    // Website you wish to allow to connect
    res.setHeader('Access-Control-Allow-Origin', 'http://localhost:3000');
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


app.get("/weather", (request, response) =>{

    let paraRange = request.query.dtRange;
    let dtNow = new Date;

    console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                    + ": " 
                    + "visiting /weather, param={" + paraRange + "}" 
                );

    switch(paraRange){
        case '1h':
            queryer.get1hWeather(response, dtNow);
            break;
        case '12h':
            queryer.get12hWeather(response, dtNow);
            break;   
        case '1d':
            queryer.get1dWeather(response, dtNow);
            break;   
        case '7d':
            queryer.get7dWeather(response, dtNow);
            break;       
        case '1m':
            queryer.get1mWeather(response, dtNow);
            break;  
        case '6m':
            queryer.get6mWeather(response, dtNow);
            break;                            
        case '1y':
            queryer.get1yWeather(response, dtNow);
            break;            
        default:
            response.send("invalid paramter")
            console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Fail; Get /weather, param={" + paraRange + "}; invalid paramter." 
                    );   
    }

});
