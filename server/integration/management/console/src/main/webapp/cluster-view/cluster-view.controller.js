'use strict';

angular.module('managementConsole')
  .controller('ClusterViewCtrl', [
    '$scope',
    'api',
    function ($scope, api) {
      api.getClusters(function(clusters) {
        $scope.$apply(function() {
          $scope.clusters = clusters;
        });
      });
      $scope.helloMsg = 'Cluster view';
  }]);