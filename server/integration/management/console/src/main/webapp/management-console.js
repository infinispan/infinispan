'use strict';

angular.module('managementConsole', [
  'managementConsole.api',
  'ui.router',
  'ui.bootstrap',
])

  .config(['$stateProvider', '$urlRouterProvider', function ($stateProvider, $urlRouterProvider) {
    $stateProvider
      .state('clusterView', {
        url: '/cluster-view',
        templateUrl: 'webapp/cluster-view/cluster-view.html',
        controller: 'ClusterViewCtrl'
      })
      .state('nodeDetails', {
        url: '/node-details',
        templateUrl: 'webapp/node-details/node-details.html',
        controller: 'NodeDetailsCtrl'
      })
      .state('error404', {
        url: '/error404',
        templateUrl: 'webapp/error404/error404.html',
      });
    $urlRouterProvider
      .when('/', '/cluster-view')
      .when('', '/cluster-view')
      .otherwise('/error404');
  }]);
