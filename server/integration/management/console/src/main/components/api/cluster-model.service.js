'use strict';

angular.module('managementConsole.api')
  .factory('ClusterModel', [
    'CacheModel',
    function (CacheModel) {
      var Cluster = function(domain, name) {
        this.domain = domain;
        this.name = name;
        this.modelController = domain.getModelController();
        this.lastRefresh = null;
        this.data = null;
        this.caches = undefined;
      };

      Cluster.prototype.getModelController = function() {
        return this.modelController;
      };

      Cluster.prototype.getResourcePath = function() {
        return this.domain.getResourcePath().concat('subsystem', 'infinispan', 'cache-container', this.name);
      };

      Cluster.prototype.refresh = function(callback) {
        this.modelController.readResource(this.getResourcePath(), false, false, function(response) {
          this.lastRefresh = new Date();
          this.data = response;
          if (callback) {
            callback(this);
          }
        }.bind(this));
      };

      Cluster.prototype.getAvailability = function() {};

      Cluster.prototype.getNodes = function() {};

      Cluster.prototype.getCaches = function() {
        var caches = [];
        var cacheTypes = ['local-cache', 'distributed-cache', 'replicated-cache', 'invalidation-cache']; 
        for(var i = 0; i < cacheTypes.length; i++) {
          var typedCaches = this.data[cacheTypes[i]];
          if (typedCaches !== undefined) {
            for(var name in typedCaches) {
              if (name !== undefined) {
                caches.push(new CacheModel(this, name, cacheTypes[i]));
              }
            }
          }
        }
        this.caches = caches;
        return caches;
      };

      return Cluster;
    }
  ]);