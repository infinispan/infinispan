'use strict';

angular.module('managementConsole.api')
  /**
   * Main service in the api module.
   */
  .factory('api', [
    'ModelController',
    'DomainModel',
    function (ModelController, DomainModel) {

      var dmrClient = new ModelController('http://localhost:3000/management', 'admin', '!qazxsw2');

      var domain = new DomainModel(dmrClient);

      /**
       * Fetches all clusters, but in order to get more data you need to refresh them manually.
       * @param callback([ClusterModel]) Callback whose first param is list of clusters.
       */
      var getClustersShallow = function(callback) {
        domain.refresh(function(d) {
          callback(d.getClusters());
        });
      };

      // Used as cache for getClustersDeep function.
      var clustersDeep;

      /**
       * Fetches all clusters and all their data, including all nodes and caches.
       * Fetched data is cached, so subsequent calls will return cached data.
       * @param callback([ClusterModel]) Callback whose first param is list of clusters.
       * @param forceRefresh (boolean) If true cache is ignored and data is refreshed.
       */
      var getClustersDeep = function(callback, forceRefresh) {
        if (clustersDeep && !forceRefresh) {
          callback(clustersDeep);
        }

        var jobsInProgress = 0;
        var jobStarted = function() {
          jobsInProgress += 1;
        };
        var jobEnded = function() {
          jobsInProgress -= 1;
          if (jobsInProgress === 0) {
            // TODO(martinsos): Although it should not happen,
            // it is theoretically possible that callback will be called multiple times.
            callback(clustersDeep);
          }
        };

        domain.refresh(function(domain) {
          clustersDeep = domain.getClusters();
          angular.forEach(clustersDeep, function(cluster) {
            jobStarted();
            cluster.refresh(function(cluster) {
              // Refresh caches.
              var caches = cluster.getCaches();
              angular.forEach(caches, function(cache) {
                jobStarted();
                cache.refresh(function() {
                  jobEnded();
                });
              });
              jobEnded();
            });
          });
        });
      };

      return {
        getClustersShallow: getClustersShallow,
        getClustersDeep: getClustersDeep
      };
    }
  ]);