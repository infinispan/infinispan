'use strict';

angular.module('managementConsole.api')
  .factory('CacheModel', [
    function () {
      var Cache = function(cluster, name, type) {
        this.cluster = cluster;
        this.name = name;
        this.type = type;
        this.modelController = cluster.getModelController();
        this.lastRefresh = null;
        this.data = null;
      };

      Cache.prototype.getModelController = function() {
        return this.modelController;
      };

      Cache.prototype.getResourcePath = function() {
        return this.cluster.getResourcePath().concat(this.type, this.name);
      };

      Cache.prototype.refresh = function(callback) {
        this.modelController.readResource(this.getResourcePath(), false, true, function(response) {
          this.data = response;
          this.lastRefresh = new Date();
          if (callback) {
            callback(this);
          }
        }.bind(this));
      };

      return Cache;
    }
  ]);
