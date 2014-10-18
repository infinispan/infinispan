'use strict';

angular.module('managementConsole.api')
  /**
   * Main service in the api module.
   */
  .factory('api', [
    'ModelController',
    'DomainModel',
    function (ModelController, DomainModel) {

      var dmrClient = new ModelController('http://localhost:9990/management', 'admin', '!qazxsw2');

      var domain = new DomainModel(dmrClient);

      var getClusters = function(callback) {
        domain.refresh(function(d) {
          callback(d.getClusters());
        });
      };

      return {
        /**
         * Fetches all clusters.
         * @param callback([ClusterModel]) Callback whose first param is list of clusters.
         */
        getClusters: getClusters
      };
    }
  ]);