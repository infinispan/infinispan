'use strict';

angular.module('managementConsole.api')
  .factory('DomainModel', [
    function () {
      var Domain = function(modelController) {
        this.modelController = modelController;
        this.lastRefresh = null;
        this.name = null;
      };

      Domain.prototype.getModelController = function() {
        return this.modelController;
      }

      Domain.prototype.getResourcePath = function() {
        return [];
      }

      Domain.prototype.refresh = function(callback) {
        this.modelController.readResource(this.getResourcePath().concat("subsystem","infinispan", "cache-container"), false, false, (function(response) {
          this.name = response.name;
          this.data = response;
          this.lastRefresh = new Date();
          callback(this);
        }).bind(this));
      }

      Domain.prototype.getClusters = function() {
        var clusters = [];
        for(var name in this.data["cache-container"]) {
          if (name != undefined) {
            clusters.push(new Cluster(this, name));
          }
        }
        return clusters;
      }

      return Domain;
    }
  ]);