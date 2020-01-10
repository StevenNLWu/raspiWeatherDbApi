
module.exports = {

    getCurrentLocaltimeInIso : function (replaceMSecond){
        var tzoffset = (new Date()).getTimezoneOffset() * 60000; //offset in milliseconds
        var localISOTime = (new Date(Date.now() - tzoffset)).toISOString().slice(0, -1);
        
        if(replaceMSecond)
            return localISOTime.replace(/\..+/, '');

        return localISOTime;
    },

    convert2IsoInLocaltimeZone : function(datetime, replaceMSecond){
        var tzoffset = datetime.getTimezoneOffset() * 60000; //offset in milliseconds
        var localISOTime = (new Date(datetime - tzoffset)).toISOString().slice(0, -1);
        
        if(replaceMSecond)
            return localISOTime.replace(/\..+/, '');

        return localISOTime;
    }
}