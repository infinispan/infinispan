// defaultWSAddress is inserted by the WS Server, which serves this script.

function Cache(cacheName, wsAddress) {

   var websocket;
   var queuedMessages = [];
   var callback;

   openWebSocket();

   function openWebSocket() {
      if (window.WebSocket) {
         if (wsAddress == null) {
            wsAddress = defaultWSAddress;
         }

         websocket = new WebSocket(wsAddress);
         // console.log("Web Socket created, state is: " + websocket.readyState);

         websocket.onopen = function(event) {
            for (i in queuedMessages) {
               send(queuedMessages[i]);
            }
            queuedMessages = null;
         }
      } else {
         alert("Sorry, cannot connect to Infinispan Cache.  Your browser does not support WebSocket.");
      }

      websocket.onmessage = function(event) {
         var jsonObj = JSON.parse(event.data);

         if (jsonObj.value != null) {
            if (jsonObj.mime == "application/json") {
               var decodedObj = JSON.parse(jsonObj.value);
               callback(jsonObj.key, decodedObj);
            } else if (jsonObj.mime == "text/plain") {
               callback(jsonObj.key, jsonObj.value);
            }
         } else {
            callback(jsonObj.key, null);
         }
      };
   }

   this.registerCallback = function (callbackFunction) {
      callback = callbackFunction;
   }

   this.put = function (key, value) {
      var encodedObject = JSON.stringify(value);
      if (encodedObject.charAt(0) == '{') {
         put(key, encodedObject, "application/json");
      } else {
         put(key, value, "text/plain");
      }
   }

   this.get = function (key) {
      var jsonObj = {
         "opCode" : "get",
         "cacheName" : cacheName,
         "key" : key
      };

      send(jsonObj);
   }

   this.remove = function (key) {
      var jsonObj = {
         "opCode" : "remove",
         "cacheName" : cacheName,
         "key" : key
      };

      send(jsonObj);
   }

   this.notify = function (key, onEvents) {
      var jsonObj = {
         "opCode" : "notify",
         "cacheName" : cacheName,
         "key" : key,
         "onEvents" : onEvents
      };

      send(jsonObj);
   }

   this.unnotify = function (key) {
      var jsonObj = {
         "opCode" : "unnotify",
         "cacheName" : cacheName,
         "key" : key
      };

      send(jsonObj);
   }

   function put(key, value, mimeType) {
      var jsonObj = {
         "opCode" : "put",
         "cacheName" : cacheName,
         "key" : key,
         "value" : value,
         "mime" : mimeType
      };

      send(jsonObj);
   }

   function send(jsonObj) {
      if (websocket.readyState == WebSocket.OPEN) {
         var jsonString = JSON.stringify(jsonObj);
         websocket.send(jsonString);
      } else {
         if (queuedMessages == null) {
            // reopen the websocket...
            openWebSocket();
            queuedMessages = [];
         }

         // console.log("Queuing operation=" + jsonObj.opCode + " because websocket state is: " + websocket.readyState);

         // Queue the message for sending once the socket is open...
         queuedMessages[queuedMessages.length] = jsonObj;
      }
   }
}
