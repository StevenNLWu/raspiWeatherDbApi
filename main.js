const Express = require("express");
const BodyParser = require("body-parser");
const MongoClient = require("mongodb").MongoClient;
const timeFormat = require("./timeFormat");
const queryer = require("./queryer");

var app = Express();
var dbConfig = require('./dbConfig');
app.use(BodyParser.json());
app.use(BodyParser.urlencoded({ extended: true }));


app.listen(5000, () => {
    MongoClient.connect(dbConfig.CONNECTION_URL, {useUnifiedTopology: true},
                                                {useNewUrlParser: true },
                                                (error, client) => {
        if(error) {
            throw error;
        }
        database = client.db(dbConfig.DATABASE_NAME);
        collection = database.collection(dbConfig.COLLECTION);
        console.log( timeFormat.getCurrentLocaltimeInIso(true)
                    + ": " 
                    +"Connected to DB `" + dbConfig.DATABASE_NAME + "`.");
    });
});

app.get("/weather", (request, response) =>{
    paraRange = request.query.dtRange;
    dtNow = new Date;

    console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                    + ": " 
                    + "Get /weather, param={" + paraRange + "}" 
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
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={" + paraRange + "}; invalid paramter." 
                    );   
    }

});
