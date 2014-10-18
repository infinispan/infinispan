'use strict';

angular.module('managementConsole.api')
  .factory('ModelController', [
    function () {
      // operations: read-resource-description, read-resource, read-attribute

      /**
       * Represents a client to the ModelController
       * @constructor
       * @param {string} url - the URL to the ModelController management endpoint
       * @param {string} username - the username to use when connecting to the management endpoint
       * @param {string} password - the password to use when connecting to the management endpoint
       */
      var ModelControllerClient = function(url, username, password) {
        this.url = url;
        this.username = username;
        this.password = password;
      };

      /**
       * Executes an operation
       * @param data
       * @param callback
       */
      ModelControllerClient.prototype.execute = function(op, callback) {
        var http = new XMLHttpRequest();
        http.withCredentials = true;
        http.open('POST', this.url, true, this.username, this.password);
        http.setRequestHeader('Content-type', 'application/json');
        http.setRequestHeader('Accept', 'application/json');
        http.onreadystatechange = function() {
          if (http.readyState === 4 && http.status === 200) {
            var response = JSON.parse(http.responseText);
            if (response.outcome === 'success') {
              if (callback) {
                callback(response.result);
              }
            } else {
              console.error(response);
            }
          }
        };
        http.send(JSON.stringify(op));
      };

      ModelControllerClient.prototype.readResource = function(address, recursive, includeRuntime, callback) {
        // parameters: RECURSIVE, RECURSIVE_DEPTH, PROXIES, INCLUDE_RUNTIME, INCLUDE_DEFAULTS, ATTRIBUTES_ONLY, INCLUDE_ALIASES
        var op = {'operation':'read-resource', 'recursive': recursive, 'include-runtime': includeRuntime , 'address': address};
        this.execute(op, callback);
      };

      ModelControllerClient.prototype.readResourceDescription = function(address, recursive, includeRuntime, callback) {
        // parameters: OPERATIONS, INHERITED, RECURSIVE, RECURSIVE_DEPTH, PROXIES, INCLUDE_ALIASES, ACCESS_CONTROL, LOCALE
        var op = {'operation':'read-resource-description', 'recursive': recursive, 'include-runtime': includeRuntime , 'address': address};
        this.execute(op, callback);
      };

      return ModelControllerClient;
    }
  ]);