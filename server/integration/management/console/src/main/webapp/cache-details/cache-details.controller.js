'use strict';

angular.module('managementConsole')
  .controller('CacheDetailsCtrl', [
    '$scope',
    'api',
    '$stateParams',
    '$state',
    function ($scope, api, $stateParams, $state) {
      if (!$stateParams.clusterName && !$stateParams.cacheName) {
        $state.go('error404');
      }

      // Set currentCache according to the url params.
      // TODO(matija): use some hashMap-like structure instead of
      //               searching through an array.
      api.getClustersDeep(function(clusters) {
        $scope.safeApply(function() {
          angular.forEach(clusters, function(cluster) {
            if (cluster.name === $stateParams.clusterName) {
              $scope.currentCluster = cluster;
              $scope.caches = cluster.caches;
            }
          });
          if ($scope.currentCluster === undefined) {
            $state.go('error404');
          }
          angular.forEach($scope.caches, function(cache) {
            if (cache.name === $stateParams.cacheName) {
              $scope.currentCache = cache;
            }
          });
        });
      });
    }]);
