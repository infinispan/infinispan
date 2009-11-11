//-- Market2Lead Inc. Website Tagging

var _m2lTracker = 
{
    // On Demand Settings
    tenantId : null,       // Tenant Id
    tenantCode : null,     // Tenant Code
    pageName : null,       // The Alias Name for the Page. Defaults to window title
    campaignCode : null,   // The Code of the Campaign for which the page belongs
    programCode : null,    // The Code of the Program for which the page belongs
    category : [],         // Categories

    // Internal Settings, But still can be configured by the user.
    _detectFlash:1,	    // set flash version detect option (1=on|0=off)
    _detectTitle:1,     // set the document title detect option (1=on|0=off)
    _detectAnchor:0,	// enable use of anchors for campaign (1=on|0=off)
    _supportHttps:1,    // set the https protocol support (1=on|0=off)

    _md: document,
    _wstImg : 'wst.gif',

    /**
     * The Main track function, which will track the Website Statistics by 
     * loading an Image and passing query parameters for that image.
     */
    track: function(){

        this._init();
        if (this.tenantCode == null) {
            this._log("tenantCode is missing for Tracking");
            return;
        }

        if(this._md.location == 'file:') return;

        var tenConfig = {
            'ti': this.tenantId,
            'tc': this.tenantCode,
            'page': this.pageName,
            'cc': this.campaignCode,
            'pc': this.programCode,
            'c1': this.category1,
            'ct' : this.category,
            'rnd' : Math.round(Math.random()*2147483647)
        }
        var bInfo = this._browserInfo();
        var pInfo = this._pageInfo();
        
        var qryStr = this._toQueryString(tenConfig, bInfo, pInfo);
        var imgUrl = this.wtBaseUrl + '/' + this._wstImg;

        var trkImg = new Image(1,1);
        trkImg.src = imgUrl + "?" + qryStr;
        //trkImg.onload=function() { this._mVoid(); }        
    },


    /**
     * Converts the given JSON arguments into a Query String
     * 
     * @param {JSON} The JSON Objects which contains Query Parameters
     *               n Number of JSON Objects can be passed.
     * 
     * @return {String} Query String i.e created from the JSON Objects
     */
    _toQueryString : function(){         
         var query = ""
         var args = arguments;

         if(!args && args.length > 0) return query;

         // Last Argument is Key 
         var current_index = null;
         if(typeof(args[args.length-1]) == 'string') {
             current_index = args[args.length-1];
         }
             
         var params = new Array();
         for(var i = 0; i < args.length; i++) 
         {
             if( typeof(args[i]) == 'object') {
                 for(key in args[i]) 
                 {
                     var data = args[i][key];
                     var key_value = key;
                     if(current_index) {
                         key_value = current_index+"["+key+"]"
                     }

                     if(data == null)  continue;

                     if(typeof(data) == 'object') {
                         /**
                            06/20/2008 - Commented out (Neelesh).  
                            For some sites, this was causing the query string to become very long
                            and the logging was failing 
                            params.push(this._toQueryString(data, key_value));
                         */                    	 
                    	 // ref bug#6835 (Vamsi)
                    	 if (key == 'ct')
                    		 params.push(this._toQueryString(data, key_value));
                     }
                     else{
                         /*
                          * If the data is a function, dont add it.
                          * if a customer is including some javascript that extends base javascript objects
                          * like arrays, this can get appended to the URL. To avoid that we check for the
                          * type of data. Neelesh 02/09
                          */
                         if (typeof(data) != 'function')
                            params.push(key_value+"="+this._encode(data));
                     }                         
                 }
             }
         }
         query = params.join("&");
         return query;
    },

    /**
     * Returns an JSON object which contains the Browser Information.
     * 
     * @return {JSON} Object cotaining the Browser Information, 
     *          JSON Object Properties
     *          bsr - Screen Resolution
     *          bsc - Screen Color
     *          bl  - Browser Language
     *          bje - Is Java Enabled ?
     *          bfv - Flash Version
     *          bcs - Charset
     */
    _browserInfo : function(){
         var sr="-",sc="-",bl="-",fl="-",cs="-",je=1;
         var n=navigator;
         if (self.screen) {
             sr=screen.width+"x"+screen.height;
             sc=screen.colorDepth+"-bit";
         } else if (self.java) {
             var j=java.awt.Toolkit.getDefaultToolkit();
             var s=j.getScreenSize();
             sr=s.width+"x"+s.height;
         }

         if (n.language) { 
             bl = n.language.toLowerCase(); 
         }  else if (n.browserLanguage) { 
             bl = n.browserLanguage.toLowerCase(); 
         }

         je = n.javaEnabled()?1:0;
         fl = this._flash();

         if (this._md.characterSet) {
             cs = this._encode(this._md.characterSet);
         }             
         else if (this._md.charset) {
             cs = this._encode(this._md.charset);
         }            

         var brInfo = {
             "bsr":sr, 
             "bsc":sc,
             "bl":bl,
             "bje":je, 
             "bfv":fl, 
             "bcs":cs
         };
         return brInfo;        
    },

    /**
     * Returns an JSON object which contains the Page Information.
     * 
     * @return {JSON} Object cotaining the Page Information
     *         JSON Object Properties
     *          pr  - Page Referer
     *          pl  - Page Location
     *          pp  - Page Protocol
     *          plh - Page Location Hash
     *          pt  - Page Title
     *          pqs - Page Query String
     */
    _pageInfo: function(){

         pl = this._md.location;

         _pr = this._md.referrer;
         _pl = this._md.location.href;
         _pp = pl.protocol;
         _plh = pl.href.substring(pl.href.indexOf('#'));
         _pt = this._encode(this._md.title);
         _pqs = this._encode(this._md.location.search);
         
         var pageInfo = {
             'pr' : _pr,           
             'pl' : _pl,
             'pp' : _pp,
             'plh': _plh,
             'pt' : _pt,
             'pqs' : _pqs
         }
         return pageInfo;
    },

    /**
     * Returns Flash Version of the Browser
     * 
     * @return The Browsers Flash Version
     */
    _flash:  function () {
         var f="-",n=navigator;
         if (n.plugins && n.plugins.length) {
             for (var ii=0;ii<n.plugins.length;ii++) {
                 if (n.plugins[ii].name.indexOf('Shockwave Flash')!=-1) {
                     f=n.plugins[ii].description.split('Shockwave Flash ')[1];
                     break;
                 }
             }
         }
         else if (window.ActiveXObject) {
             for (var ii=10;ii>=2;ii--) {
                 try {
                     var fl=eval("new ActiveXObject('ShockwaveFlash.ShockwaveFlash."+ii+"');");
                     if (fl) { 
                        f=ii + '.0'; 
                        break; 
                     }
                 }
                 catch(e) {}
             }
         }
         return f;
    },

    /**
     * Encodes the given String
     * 
     * @param {String} The String that has to be Encoded
     * 
     * @return {String} Encoded String
     */
    _encode: function (s,u) {
         if (typeof(encodeURIComponent) == 'function') {
             if (u) 
                 return encodeURI(s);
             else 
                 return encodeURIComponent(s);
         } 
         else {
             return escape(s);
         }
    },

    /**
     * Decodes the given String
     * 
     * @param {String} The String that has to be Decoded
     * 
     * @return {String} Decoded String
     */
    _decode : function (s) {
         if (typeof(decodeURIComponent) == 'function') {
             return decodeURIComponent(s);
         } else {
             return unescape(s);
         }
    },

    _mVoid : function(){
         return;
    },

    _log: function(msg){
         alert(msg);
    },


    _init: function(){

         if(document.getElementById("m2lwst")) {
             var scriptSrc = document.getElementById("m2lwst").src;
             if(scriptSrc) {
                 var lastIndex = scriptSrc.lastIndexOf("/");
                 this.wtBaseUrl = scriptSrc.substring(0,lastIndex);
             }
         }

         if(!this.wtBaseUrl) {
             this.wtBaseUrl = 'http://m2l.market2lead.com/wt';
         }
    }
}
