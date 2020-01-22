const Timer = require("./timer");
const timer = new Timer();

module.exports = class Queryer{

    constructor() {
        
        // make sure 'this' is pointing to this class
        this._samplingPermin = this._samplingPermin.bind(this);
        this._samplingPer2min = this._samplingPer2min.bind(this);  
        this._samplingPer5min = this._samplingPer5min.bind(this);       
        this._samplingPerHour = this._samplingPerHour.bind(this);             
        this._samplingPer2hr = this._samplingPer2hr.bind(this);           
        this._samplingPerDay = this._samplingPerDay.bind(this);        
      }

    _samplingPermin(dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timer.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timer.convert2IsoInLocaltimeZone(dtTo, true), 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDatetime"
                                }
                            }
                        }
                    },
                    {   // prepare to group by min
                        $project: {
                            _id: "$id",
                            year: { $year: "$2dateTime" },
                            month: { $month: "$2dateTime" },
                            day: { $dayOfMonth: "$2dateTime" },
                            hr: {"$hour":"$2dateTime"},
                            min: {"$minute":"$2dateTime"},           
                            temperature: "$temperature",
                            humidity: "$humidity",
                            pressure: "$pressure"
                        }
                    },
                    {   // group by min
                        $group: {
                            _id:{
                                year: "$year",
                                month: "$month",
                                day: "$day",
                                hr: "$hr",
                                min: "$min"
                            },
                            avgTemp: { $avg: "$temperature" },
                            avgHum: { $avg: "$humidity"},
                            avgPrs: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    } // _samplingPermin

    _samplingPer2min(dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timer.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timer.convert2IsoInLocaltimeZone(dtTo, true), 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDatetime"
                                }
                            }
                        }
                    },
                    {   // prepare to group by 2min
                        $project: {
                            _id: "$id",
                            year: { $year: "$2dateTime" },
                            month: { $month: "$2dateTime" },
                            day: { $dayOfMonth: "$2dateTime" },
                            hr: {"$hour":"$2dateTime"},
                            min: {"$minute":"$2dateTime"},  
                            "min/2": {$floor:{
                                $divide: [{"$minute":"$2dateTime"}, 2]
                            }},                
                            temperature: "$temperature",
                            humidity: "$humidity",
                            pressure: "$pressure"
                        }
                    },
                    {   // group by 2min
                        $group: {
                            _id:{
                                year: "$year",
                                month: "$month",
                                day: "$day",
                                hr: "$hr",
                                "min/2": "$min/2"
                            },
                            avgTemp: { $avg: "$temperature" },
                            avgHum: { $avg: "$humidity"},
                            avgPrs: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    } // end of _samplingPer2min

    _samplingPer5min(dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timer.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timer.convert2IsoInLocaltimeZone(dtTo, true), 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDatetime"
                                }
                            }
                        }
                    },
                    {   // prepare to group 5min
                        $project: {
                            _id: "$id",
                            year: { $year: "$2dateTime" },
                            month: { $month: "$2dateTime" },
                            day: { $dayOfMonth: "$2dateTime" },
                            hr: {"$hour":"$2dateTime"},
                            min: {"$minute":"$2dateTime"},  
                            "min/5": {$floor:{
                                $divide: [{"$minute":"$2dateTime"}, 5]
                            }},                
                            temperature: "$temperature",
                            humidity: "$humidity",
                            pressure: "$pressure"
                        }
                    },
                    {   // group by 5min
                        $group: {
                            _id:{
                                year: "$year",
                                month: "$month",
                                day: "$day",
                                hr: "$hr",
                                "min/5": "$min/5"
                            },
                            avgTemp: { $avg: "$temperature" },
                            avgHum: { $avg: "$humidity"},
                            avgPrs: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    } // end of _samplingPer5min

    _samplingPerHour(dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timer.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timer.convert2IsoInLocaltimeZone(dtTo, true), 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDatetime"
                                }
                            }
                        }
                    },
                    {   // prepare to group by hour
                        $project: {
                            _id: "$id",
                            year: { $year: "$2dateTime" },
                            month: { $month: "$2dateTime" },
                            day: { $dayOfMonth: "$2dateTime" },
                            hr:{"$hour":"$2dateTime"},
                            temperature: "$temperature",
                            humidity: "$humidity",
                            pressure: "$pressure"
                        }
                    },
                    {   // group by hour
                        $group: {
                            _id:{
                                year: "$year",
                                month: "$month",
                                day: "$day",
                                hour: "$hr",
                            },
                            avgTemp: { $avg: "$temperature" },
                            avgHum: { $avg: "$humidity"},
                            avgPrs: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    } // end of _samplingPerHour

    _samplingPer2hr(dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timer.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timer.convert2IsoInLocaltimeZone(dtTo, true), 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDatetime"
                                }
                            }
                        }
                    },
                    {   // prepare to group by 2hr
                        $project: {
                            _id: "$id",
                            year: { $year: "$2dateTime" },
                            month: { $month: "$2dateTime" },
                            day: { $dayOfMonth: "$2dateTime" },
                            hr:{"$hour":"$2dateTime"},
                            "hr/2":{$floor:{
                                        $divide: [{"$hour":"$2dateTime"}, 2]
                                    }},
                            temperature: "$temperature",
                            humidity: "$humidity",
                            pressure: "$pressure"
                        }
                    },
                    {   // group by 2hr
                        $group: {
                            _id:{
                                year: "$year",
                                month: "$month",
                                day: "$day",
                                "hr/2": "$hr/2",
                            },
                            avgTemp: { $avg: "$temperature" },
                            avgHum: { $avg: "$humidity"},
                            avgPrs: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    } // end of _samplingPer2hr

    _samplingPerDay(dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timer.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timer.convert2IsoInLocaltimeZone(dtTo, true), 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDatetime"
                                }
                            }
                        }
                    },
                    {   // prepare to group by day
                        $project: {
                            _id: "$id",
                            year: { $year: "$2dateTime" },
                            month: { $month: "$2dateTime" },
                            day: { $dayOfMonth: "$2dateTime" },
                            temperature: "$temperature",
                            humidity: "$humidity",
                            pressure: "$pressure"
                        }
                    },
                    {   // group by day
                        $group: {
                            _id:{
                                year: "$year",
                                month: "$month",
                                day: "$day",
                            },
                            avgTemp: { $avg: "$temperature" },
                            avgHum: { $avg: "$humidity"},
                            avgPrs: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    } // end of _samplingPerDay

    _getDbSize(callMeback){

        // Retrieve the statistics for the collection
        collection.stats((error, stats) =>{
            if(error){
                callMeback("size error");
            }
            callMeback(stats.size);
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-hour weather data
    /* 
    *********************************************/
    get1hWeather(response, dtNow){
        
        this._getDbSize( (sizeResult)=>{
            
            let dtFrom = new Date();
            dtFrom.setTime(dtNow.getTime()- 1*60*60*1000);    // in milliseconds; minus 1 hour
    
            this._samplingPermin(dtFrom, dtNow).toArray((error, weatherResult) =>{
                if(error){
    
                    console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                                + ": " 
                                + "Fail; Get /weather, param={1h}; " 
                                + error.toString() 
                                );
                    response.json({ 
                                status: "error",
                                message: error.toString(),
                                });
                    return  response.status(500).send(error);
                }
                response.json({ 
                                status: "success",
                                message: "success",
                                data: weatherResult,
                                "size": sizeResult 
                            });
                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Success; Get /weather, param={1h}." 
                            );
            });
        }); // end of _getDbSize's callback
    }

    /***************************************** */
    /*
    /*   function to get 12-hour weather data
    /* 
    *********************************************/
    get12hWeather(response, dtNow){

        let dtFrom = new Date();
        dtFrom.setTime(dtNow.getTime() - 12*60*60*1000);    // in milliseconds; minus 12 hour

        this._samplingPer2min(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Fail ; Get /weather, param={12h}; "
                            + error.toString() 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Success; Get /weather, param={12h}." 
                        );
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-day weather data
    /* 
    *********************************************/
    get1dWeather(response, dtNow){

        let dtFrom = new Date();
        dtFrom.setTime( dtNow.getTime() - 1 * 86400000 );    // minus 1 day

        this._samplingPer5min(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Fail; Get /weather, param={1d}; "
                            + error.toString() 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Success; Get /weather, param={1d}." 
                        );
        });
    }

    /***************************************** */
    /*
    /*   function to get 7-day weather data
    /* 
    *********************************************/
    get7dWeather(response, dtNow){

        let dtFrom = new Date();
        dtFrom.setTime( dtNow.getTime() - 7 * 86400000 );    // minus 7 day

        this._samplingPerHour(dtFrom, dtNow).toArray((error, result) =>{
            if(error){
                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Fail; Get /weather, param={7d}; "
                            + error.toString() 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Success; Get /weather, param={7d}." 
                        );
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-month weather data
    /* 
    *********************************************/
    get1mWeather(response, dtNow){

        let dtFrom = new Date();
        dtFrom.setMonth(dtNow.getMonth() - 1);    // minus 1 month

        this._samplingPer2hr(dtFrom, dtNow).toArray((error, result) =>{
                if(error){

                    console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                                + ": " 
                                + "Fail; Get /weather, param={1m}; "
                                + error.toString() 
                                );
                    return  response.status(500).send(error);
                }
                response.send(result);
                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Success; Get /weather, param={1m}." 
                            );
        });
    }

    /***************************************** */
    /*
    /*   function to get 6-month weather data
    /* 
    *********************************************/
    get6mWeather(response, dtNow){

        let dtFrom = new Date();
        dtFrom.setMonth(dtNow.getMonth() - 6);    // minus 6 month

        this._samplingPerDay(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Fail; Get /weather, param={6m};"
                            + error.toString() 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Success; Get /weather, param={6m}." 
                        );
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-year weather data
    /* 
    *********************************************/
    get1yWeather(response, dtNow){

        let dtFrom = new Date();
        dtFrom.setFullYear(dtNow.getFullYear() - 1);    // minus 1 year

        this._samplingPerDay(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Fail; Get /weather, param={1y}; "
                            + error.toString() 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timer.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Success; Get /weather, param={1y}." 
                        );
        });
    }

}; // end of class

