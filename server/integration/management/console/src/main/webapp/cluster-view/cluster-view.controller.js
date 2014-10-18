'use strict';

angular.module('managementConsole')
  .controller('ClusterViewCtrl', [
    '$scope',
    'api',
    '$stateParams',
    '$state',
    function ($scope, api, $stateParams, $state) {
      $scope.shared = {
        currentCollection: 'caches'
      };
      $scope.clusters = undefined;
      $scope.currentCluster = undefined;

      // Fetch all clusters and their caches.
      api.getClustersDeep(function(clusters) {
        $scope.safeApply(function() {
          $scope.clusters = clusters;
          if ($stateParams.clusterName) {
            angular.forEach(clusters, function(cluster) {
              if (cluster.name === $stateParams.clusterName) {
                $scope.currentCluster = cluster;
              }
            });
            if ($scope.currentCluster === undefined) {
              $state.go('error404');
            }
          } else {
            $state.go('clusterView', {'clusterName': clusters[0].name});
          }
        });
      });

      $scope.$watch('currentCluster', function(currentCluster) {
        if (currentCluster && currentCluster.name !== $stateParams.clusterName) {
          $state.go('clusterView', {'clusterName': currentCluster.name});
        }
      });
  }]);