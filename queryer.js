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
                            "uploadDtInUtc":{
                                "$gte": timer.convert2Iso(dtFrom, false, true),
                                "$lte": timer.convert2Iso(dtTo, false, true) 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDtInUtc"
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
                            hr: {$hour: "$2dateTime"},
                            min: {$minute: "$2dateTime"},     
                            device: "$device",      
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
                                hour: "$hr",
                                min: "$min",
                                device:"$device",
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
                            "uploadDtInUtc":{
                                "$gte": timer.convert2Iso(dtFrom, false, true),
                                "$lte": timer.convert2Iso(dtTo, false, true) 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDtInUtc"
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
                            hr: {$hour :"$2dateTime"},
                            min: {$minute :"$2dateTime"},  
                            "min/2": {$floor:{
                                $divide: [{"$minute":"$2dateTime"}, 2]
                            }},    
                            device: "$device",            
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
                                hour: "$hr",
                                "min/2": "$min/2",
                                device: "$device",
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
                            "uploadDtInUtc":{
                                "$gte": timer.convert2Iso(dtFrom, false, true),
                                "$lte": timer.convert2Iso(dtTo, false, true) 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDtInUtc"
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
                            hr: {$hour :"$2dateTime"},
                            min: {$minute :"$2dateTime"},  
                            "min/5": {$floor:{
                                $divide: [{"$minute":"$2dateTime"}, 5]
                            }},            
                            device: "$device",    
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
                                hour: "$hr",
                                "min/5": "$min/5",
                                device: "$device",
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
                            "uploadDtInUtc":{
                                "$gte": timer.convert2Iso(dtFrom, false, true),
                                "$lte": timer.convert2Iso(dtTo, false, true) 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDtInUtc"
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
                            hr:{$hour: "$2dateTime"},
                            device: "$device",
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
                                min: "0",
                                device: "$device",
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
                            "uploadDtInUtc":{
                                "$gte": timer.convert2Iso(dtFrom, false, true),
                                "$lte": timer.convert2Iso(dtTo, false, true) 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDtInUtc"
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
                            hr:{$hour: "$2dateTime"},
                            "hr/2":{$floor:{
                                        $divide: [{$hour:"$2dateTime"}, 2]
                                    }}, 
                            device: "$device",
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
                                "hour/2": "$hr/2",
                                min: "0",
                                device: "$device",
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
                            "uploadDtInUtc":{
                                "$gte": timer.convert2Iso(dtFrom, false, true),
                                "$lte": timer.convert2Iso(dtTo, false, true) 
                            }
                        }
                    },
                    {   // string to datetime
                        $addFields: {
                            "2dateTime":{
                                "$dateFromString": { 
                                    "dateString": "$uploadDtInUtc"
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
                            device: "$device",
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
                                hour: "0",
                                min: "0",
                                device: "$device",      
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

    _latestRecord(){
        return  collection.aggregate([{
            $addFields: { // string to datetime
                        "2dateTime":{
                            "$dateFromString": { 
                                "dateString": "$uploadDtInUtc"
                            } 
                        }
                    }
            },
            {   // sorting
                $sort: {"device": 1,
                        "2dateTime": 1
                }
            },
            {   // group by device and get the last updated record
                $group: {
                        _id: "$device",
                        lastUpdated: {$last: "$2dateTime"},
                        temp: {$last: "$temperature"},
                        hum: {$last: "$humidity"},
                        prs: {$last: "$pressure"},            
                }
            },
        ], 
            {allowDiskUse: true}
        );
} // end of _latestRecord

    _getDbSize(callMeback){

        // Retrieve the statistics for the collection
        collection.stats((error, stats) =>{
            if(error){
                callMeback("size error");
                return;
            }
            callMeback(stats.size);
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-hour weather data
    /* 
    *********************************************/
    get1hWeather(ip, response, dtNow){
   
        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

            this._getDbSize( (sizeResult)=>{
                
                let dtNowUtc = new Date();
                let dtFromUtc = new Date();           
                dtFromUtc.setTime(dtNowUtc.getTime()- 1*60*60*1000);    // in milliseconds; minus 1 hour
        
                this._samplingPermin(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                    if(error){
        
                        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                    + ": " 
                                    + ip 
                                    + "; "
                                    + "Fail; Get /weather, param={1h}; " 
                                    + error.toString() 
                                    );
                        return response.status(500).json({ 
                                    status: "error",
                                    message: error.toString(),
                                    });
                    }
                    response.json({ 
                                    status: "success",
                                    message: "success",
                                    data: weatherResult,
                                    lastRecord: lastRecord,
                                    "size": sizeResult 
                                });
                    console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                + ": " 
                                + ip 
                                + "; "
                                + "Success; Get /weather, param={1h}." 
                                );
                });
            }); // end of _getDbSize's callback
        }); // end of _latestRecord's callback
    }

    /***************************************** */
    /*
    /*   function to get 12-hour weather data
    /* 
    *********************************************/
    get12hWeather(ip, response, dtNow){

        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

            this._getDbSize( (sizeResult)=>{
                
                let dtNowUtc = new Date();
                let dtFromUtc = new Date();      
                dtFromUtc.setTime(dtNowUtc.getTime() - 12*60*60*1000);    // in milliseconds; minus 12 hour

                this._samplingPer2min(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                    if(error){

                        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                    + ": " 
                                    + ip 
                                    + "; "
                                    + "Fail ; Get /weather, param={12h}; "
                                    + error.toString() 
                                    );
                        return response.status(500).json({ 
                                        status: "error",
                                        message: error.toString(),
                                    });
                    }
                    response.json({ 
                                    status: "success",
                                    message: "success",
                                    data: weatherResult,
                                    lastRecord: lastRecord,
                                    "size": sizeResult 
                                });
                    console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                + ": " 
                                + ip 
                                + "; "
                                + "Success; Get /weather, param={12h}." 
                                );
                });
            });
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-day weather data
    /* 
    *********************************************/
    get1dWeather(ip, response, dtNow){

        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

            this._getDbSize( (sizeResult)=>{
                 
                let dtNowUtc = new Date();
                let dtFromUtc = new Date();    
                dtFromUtc.setTime( dtNowUtc.getTime() - 1 * 86400000 );    // minus 1 day

                this._samplingPer5min(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                    if(error){

                        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                    + ": " 
                                    + ip 
                                    + "; "
                                    + "Fail; Get /weather, param={1d}; "
                                    + error.toString() 
                                    );
                        return response.status(500).json({ 
                                        status: "error",
                                        message: error.toString(),
                                    });
                    }
                    response.json({ 
                                    status: "success",
                                    message: "success",
                                    data: weatherResult,
                                    lastRecord: lastRecord,
                                    "size": sizeResult 
                                });
                    console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                + ": " 
                                + ip 
                                + "; "
                                + "Success; Get /weather, param={1d}." 
                                );
                });
            });
        });
    }

    /***************************************** */
    /*
    /*   function to get 7-day weather data
    /* 
    *********************************************/
    get7dWeather(ip, response, dtNow){


        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

            this._getDbSize( (sizeResult)=>{
                
                let dtNowUtc = new Date();
                let dtFromUtc = new Date();    
                dtFromUtc.setTime( dtNowUtc.getTime() - 7 * 86400000 );    // minus 7 day

                this._samplingPerHour(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                    if(error){
                        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                    + ": " 
                                    + ip 
                                    + "; "
                                    + "Fail; Get /weather, param={7d}; "
                                    + error.toString() 
                                    );
                        return response.status(500).json({ 
                                        status: "error",
                                        message: error.toString(),
                                    });
                    }
                    response.json({ 
                                    status: "success",
                                    message: "success",
                                    data: weatherResult,
                                    lastRecord: lastRecord,
                                    "size": sizeResult 
                                });
                    console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                + ": " 
                                + ip 
                                + "; "
                                + "Success; Get /weather, param={7d}." 
                                );
                });
            });
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-month weather data
    /* 
    *********************************************/
    get1mWeather(ip, response, dtNow){

        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

            this._getDbSize( (sizeResult)=>{
                
                let dtNowUtc = new Date();
                let dtFromUtc = new Date();   
                dtFromUtc.setMonth(dtNowUtc.getMonth() - 1);    // minus 1 month

                this._samplingPer2hr(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                        if(error){

                            console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                        + ": " 
                                        + ip 
                                        + "; "
                                        + "Fail; Get /weather, param={1m}; "
                                        + error.toString() 
                                        );
                            return response.jstatus(500).json({ 
                                            status: "error",
                                            message: error.toString(),
                                        });
                        }
                        response.json({ 
                                        status: "success",
                                        message: "success",
                                        data: weatherResult,
                                        lastRecord: lastRecord,
                                        "size": sizeResult 
                                    });
                        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                    + ": " 
                                    + ip 
                                    + "; "
                                    + "Success; Get /weather, param={1m}." 
                                    );
                });
            });
        });
    }

    /***************************************** */
    /*
    /*   function to get 6-month weather data
    /* 
    *********************************************/
    get6mWeather(ip, response, dtNow){

        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

        this._getDbSize( (sizeResult)=>{            

            let dtNowUtc = new Date();
            let dtFromUtc = new Date();  
            dtFromUtc.setMonth(dtNow.getMonth() - 6);    // minus 6 month

            this._samplingPerDay(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                if(error){

                    console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                + ": " 
                                + ip 
                                + "; "
                                + "Fail; Get /weather, param={6m};"
                                + error.toString() 
                                );
                    return response.status(500).json({ 
                                    status: "error",
                                    message: error.toString(),
                                });
                }
                response.json({ 
                                status: "success",
                                message: "success",
                                data: weatherResult,
                                lastRecord: lastRecord,
                                "size": sizeResult 
                            });
                console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                            + ": " 
                            + ip 
                            + "; "
                            + "Success; Get /weather, param={6m}." 
                            );
                });
            });
        });
    }

    /***************************************** */
    /*
    /*   function to get 1-year weather data
    /* 
    *********************************************/
    get1yWeather(ip, response, dtNow){

        this._latestRecord().toArray((error, lastRecord) =>{
            if(error){
                lastRecord = "null";
            }

            this._getDbSize( (sizeResult)=>{
                    
                let dtNowUtc = new Date();
                let dtFromUtc = new Date();  
                dtFromUtc.setFullYear(dtNow.getFullYear() - 1);    // minus 1 year

                this._samplingPerDay(dtFromUtc, dtNowUtc).toArray((error, weatherResult) =>{
                    if(error){

                        console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                    + ": " 
                                    + ip 
                                    + "; "
                                    + "Fail; Get /weather, param={1y}; "
                                    + error.toString() 
                                    );
                        return response.status(500).json({ 
                                        status: "error",
                                        message: error.toString(),
                                    });
                    }
                    response.json({ 
                                    status: "success",
                                    message: "success",
                                    data: weatherResult,
                                    lastRecord: lastRecord,
                                    "size": sizeResult 
                                });
                    console.log( timer.convert2IsoInLocaltimeZone(dtNow)
                                + ": " 
                                + ip 
                                + "; "
                                + "Success; Get /weather, param={1y}." 
                                );
                });
            });
        });
    }

}; // end of class
