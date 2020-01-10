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
    dtQuerryUntil = new Date(dtNow);
    switch(paraRange){
        case '1h':
            queryer.get1hWeather(response, dtQuerryUntil);
            break;
        case '12h':
            dtQuerryUntil.setTime(dtQuerryUntil.getTime() - 12*60*60*1000);    // in milliseconds; minus 12 hour
            break;   
        case '1d':
            dtQuerryUntil.setTime(dtQuerryUntil.getDate() - 1);    // minus 1 day
            break;   
        case '7d':
            dtQuerryUntil.setTime(dtQuerryUntil.getDate() - 7);    // minus 7 day
            break;       
        case '1m':
            dtQuerryUntil.setTime(dtQuerryUntil.getMonth() - 1);    // minus 1 month
            break;  
        case '6m':
            dtQuerryUntil.setTime(dtQuerryUntil.getMonth() - 6);    // minus 6 month
            break;                            
        case '1y':
            queryer.get1yWeather(response, dtQuerryUntil);
            break;            
        default:
            dtQuerryUntil.setTime(dtQuerryUntil.getTime() - 1*60*60*1000);
    }

    console.log( 
        timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
        + ": " 
        + "Get /weather, param={" + paraRange + "}" 
    );
});

app.get("/string2Double", (request, response) =>{

    collection.find({$or:[ {temperature: {$type: 2}}
                            ,{humidity: {$type: 2}}
                            ,{pressure: {$type: 2}}
                        ]
                    }).forEach(function(x) {

        x.temperature = parseFloat(x.temperature);
        x.humidity = parseFloat(x.humidity);
        x.pressure = parseFloat(x.pressure);

        console.log( x._id.toString());

        collection.updateOne(x, (error, result) =>{
            if(error){
                return  response.status(500).send(error);
            }

            response.send(result);
        })
    })
});

app.get("/temperature/string2Double", (request, response) =>{

    collection.find({temperature: {$type: 2}}).forEach(function(x) {

        x.temperature = parseFloat(x.temperature);

        collection.updateOne(   {_id: x._id}, 
                                { $set: {temperature:parseFloat(x.temperature) }}
                            );
        response.write( x._id.toString()+ '\r\n');
        console.log( x._id.toString());
    })
});

app.get("/humidity/string2Double", (request, response) =>{

    collection.find({"humidity": {$type: 2}}).forEach(function(x) {

        x.humidity = parseFloat(x.humidity);

        collection.updateOne(   {_id: x._id}, 
                                { $set: {humidity:parseFloat(x.humidity) }}
                            );
        response.write( x._id.toString()+ '\r\n');
        console.log( x._id.toString());
    })
});


app.get("/pressure/string2Double", (request, response) =>{

    collection.find({pressure: {$type: 2}}).forEach(function(x) {

        x.pressure = parseFloat(x.pressure);


        collection.updateOne(   {_id: x._id}, 
                                { $set: {pressure:parseFloat(x.pressure) }}
                            );
        response.write( x._id.toString() + '\r\n');
        console.log( x._id.toString());
    })
});