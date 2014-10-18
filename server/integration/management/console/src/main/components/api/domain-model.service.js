'use strict';

angular.module('managementConsole.api')
  .factory('DomainModel', [
    'ClusterModel',
    function (ClusterModel) {
      var Domain = function(modelController) {
        this.modelController = modelController;
        this.lastRefresh = null;
        this.name = null;
      };

      Domain.prototype.getModelController = function() {
        return this.modelController;
      };

      Domain.prototype.getResourcePath = function() {
        return [];
      };

      Domain.prototype.refresh = function(callback) {
        this.modelController.readResource(this.getResourcePath().concat(
            'subsystem', 'infinispan', 'cache-container'), false, false, function(response) {
          this.name = response.name;
          this.data = response;
          this.lastRefresh = new Date();
          if (callback) {
            callback(this);
          }
        }.bind(this));
      };

      Domain.prototype.getClusters = function() {
        var clusters = [];
        for(var name in this.data['cache-container']) {
          if (name !== undefined) {
            clusters.push(new ClusterModel(this, name));
          }
        }
        return clusters;
      };

      return Domain;
    }
  ]);