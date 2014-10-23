'use strict';

angular.module('managementConsole', [
  'managementConsole.api',
  'ui.router',
  'ui.bootstrap',
])

  .config(['$stateProvider', '$urlRouterProvider', function ($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('clusterView', {
        // TODO(martinsos): I am assuming here that cluster names are unique!
        // If not, we should use some other identifier.
        url: '/cluster/:clusterName',
        templateUrl: 'webapp/cluster-view/cluster-view.html',
        controller: 'ClusterViewCtrl'
      })
      .state('nodeDetails', {
        url: '/cluster/:clusterName/node/:nodeName',
        templateUrl: 'webapp/node-details/node-details.html',
        controller: 'NodeDetailsCtrl'
      })
      .state('cacheDetails', {
        url: '/cluster/:clusterName/cache/:cacheName',
        templateUrl: 'webapp/cache-details/cache-details.html',
        controller: 'CacheDetailsCtrl'
      })
      .state('error404', {
        url: '/error404',
        templateUrl: 'webapp/error404/error404.html',
      });
    $urlRouterProvider
      .when('/', '/cluster/')
      .when('', '/cluster/')
      .otherwise('/error404');
  }])

  /**
   * Safe apply method. It should be used when normal $scope.$apply happens to execute
   * during digest cycle which causes an error.
   * Use it just like normal apply: $scope.safeApply(myFunc).
   */
  .run(['$rootScope', '$timeout', function($rootScope, $timeout) {
    $rootScope.safeApply = function(f) {
      var scope = this;
      $timeout(function() {
        scope.$apply(f);
      });
    };
  }]);
