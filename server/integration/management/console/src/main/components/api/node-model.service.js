'use strict';

angular.module('managementConsole.api')
  .factory('NodeModel', [
    function () {
      /**
       * Represents a Node
       */
      var Node = function(name, modelController) {
        this.name = name;
        this.modelController = modelController;
        this.lastRefresh = null;
      };

      Node.prototype.refresh = function() {};

      return Node;
    }
  ]);