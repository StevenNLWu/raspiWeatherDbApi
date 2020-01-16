const timeFormat = require("./timeFormat");

module.exports = {

    /***************************************** */
    /*
    /*   function to get 1-hour weather data
    /* 
    *********************************************/
    get1hWeather : function (response, dtNow){
 
        dtFrom = new Date();
        dtFrom.setTime(dtNow.getTime()- 1*60*60*1000);    // in milliseconds; minus 1 hour

        collection.find({
            "uploadDatetime":{
                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                "$lte": timeFormat.getCurrentLocaltimeInIso(true)
            }
        }).toArray((error, result) =>{
            if(error){

                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={1h}; fail." 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={1h}; success." 
                        );
        });
    },

    /***************************************** */
    /*
    /*   function to get 12-hour weather data
    /* 
    *********************************************/
    get12hWeather : function (response, dtNow){

        dtFrom = new Date();
        dtFrom.setTime(dtNow.getTime() - 12*60*60*1000);    // in milliseconds; minus 12 hour

        module.exports.samplingPer2min(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={12h}; fail." 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={12h}; success." 
                        );
        });
    },

    /***************************************** */
    /*
    /*   function to get 1-day weather data
    /* 
    *********************************************/
    get1dWeather : function (response, dtNow){

        dtFrom = new Date();
        dtFrom.setTime( dtNow.getTime() - 1 * 86400000 );    // minus 1 day

        module.exports.samplingPer5min(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={1d}; fail." 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={1d}; success." 
                        );
        });
    },

    /***************************************** */
    /*
    /*   function to get 7-day weather data
    /* 
    *********************************************/
    get7dWeather : function (response, dtNow){

        dtFrom = new Date();
        dtFrom.setTime( dtNow.getTime() - 7 * 86400000 );    // minus 7 day

        module.exports.samplingPerHour(dtFrom, dtNow).toArray((error, result) =>{
            if(error){
                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={7d}; fail." 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={7d}; success." 
                        );
        });
    },

    /***************************************** */
    /*
    /*   function to get 1-month weather data
    /* 
    *********************************************/
    get1mWeather : function (response, dtNow){

        dtFrom = new Date();
        dtFrom.setMonth(dtNow.getMonth() - 1);    // minus 1 month

        module.exports.samplingPer2hr(dtFrom, dtNow).toArray((error, result) =>{
                if(error){

                    console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                                + ": " 
                                + "Get /weather, param={1m}; fail." 
                                );
                    return  response.status(500).send(error);
                }
                response.send(result);
                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={1m}; success." 
                            );
        });
    },

    /***************************************** */
    /*
    /*   function to get 6-month weather data
    /* 
    *********************************************/
    get6mWeather : function (response, dtNow){

        dtFrom = new Date();
        dtFrom.setMonth(dtNow.getMonth() - 6);    // minus 6 month

        module.exports.samplingPerday(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={6m}; fail." 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={6m}; success." 
                        );
        });
    },

    /***************************************** */
    /*
    /*   function to get 1-year weather data
    /* 
    *********************************************/
    get1yWeather : function (response, dtNow){

        dtFrom = new Date();
        dtFrom.setFullYear(dtNow.getFullYear() - 1);    // minus 1 year

        module.exports.samplingPerday(dtFrom, dtNow).toArray((error, result) =>{
            if(error){

                console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                            + ": " 
                            + "Get /weather, param={1y}; fail." 
                            );
                return  response.status(500).send(error);
            }
            response.send(result);
            console.log( timeFormat.convert2IsoInLocaltimeZone(dtNow, true)
                        + ": " 
                        + "Get /weather, param={1y}; success." 
                        );
        });
    },


    samplingPerDay : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPerDay

    samplingPer2hr : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPer2hr

    samplingPerHour : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPerHour

    samplingPer5min : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPer5min
 
    samplingPer2min : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                    {   // prepare to group 2min
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPer2min
    samplingPer5min : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPer5min
 
    samplingPermin : function (dtFrom, dtTo){
        return  collection.aggregate([{
                    $match: {   // filter by date range
                            "uploadDatetime":{
                                "$gte": timeFormat.convert2IsoInLocaltimeZone(dtFrom, true), 
                                "$lte": timeFormat.convert2IsoInLocaltimeZone(dtTo, true), 
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
                    {   // prepare to group 2min
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
                            avgAvg: { $avg: "$pressure"}             
                        }
                    },
                    {   // sort by datetime
                        $sort: {
                            _id:-1
                        }
                    }
                ])
    }, // samplingPer2min

} // end of export

