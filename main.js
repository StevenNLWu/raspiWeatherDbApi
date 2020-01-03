const Express = require("express");
const BodyParser = require("body-parser");
const MongoClient = require("mongodb").MongoClient;

var app = Express();
var dbConfig = require('./dbConfig');
app.use(BodyParser.json());
app.use(BodyParser.urlencoded({ extended: true }));


function getCurrentLocaltimeInIso(replaceMSecond){
    var tzoffset = (new Date()).getTimezoneOffset() * 60000; //offset in milliseconds
    var localISOTime = (new Date(Date.now() - tzoffset)).toISOString().slice(0, -1);
    
    if(replaceMSecond)
        return localISOTime.replace(/\..+/, '');

    return localISOTime;
}

function convert2IsoInLocaltimeZone(datetime, replaceMSecond){
    var tzoffset = datetime.getTimezoneOffset() * 60000; //offset in milliseconds
    var localISOTime = (new Date(datetime - tzoffset)).toISOString().slice(0, -1);
    
    if(replaceMSecond)
        return localISOTime.replace(/\..+/, '');

    return localISOTime;
}



app.listen(5000, () => {
    MongoClient.connect(dbConfig.CONNECTION_URL, {useUnifiedTopology: true},
                                                {useNewUrlParser: true },
                                                (error, client) => {
        if(error) {
            throw error;
        }
        database = client.db(dbConfig.DATABASE_NAME);
        collection = database.collection("weather_data");
        console.log( getCurrentLocaltimeInIso(true)
                    + ": " 
                    +"Connected to DB `" + dbConfig.DATABASE_NAME + "`.");
    });
});

app.get("/weather", (request, response) =>{
    collection.find({}).toArray((error, result) =>{
        if(error){
            return  response.status(500).send(error);
        }
        response.send(result);
    });
});

app.get("/weather/:dtRange", (request, response) =>{
    dtRange = request.params.dtRange;
    dtQuerryUntil = new Date()
    switch(dtRange){
        case '1h':
            dtQuerryUntil.setTime(dtQuerryUntil.getTime()- 1*60*60*1000);    // in milliseconds
            break;
        default:
            dtQuerryUntil.setTime(dtQuerryUntil.getTime() - 1*60*60*1000);
    }

    console.log( convert2IsoInLocaltimeZone(dtQuerryUntil, true));
    console.log( getCurrentLocaltimeInIso(true));
    collection.find({
            "uploadDatetime":{
                "$gte": convert2IsoInLocaltimeZone(dtQuerryUntil, true), "$lte": getCurrentLocaltimeInIso(true)
            }
        }).toArray((error, result) =>{
        if(error){
            return  response.status(500).send(error);
        }
        response.send(result);
    });
});