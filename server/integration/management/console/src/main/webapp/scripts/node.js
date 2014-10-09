/**
 * Represents a Node
 */
function Node(name, modelController) {
   this.name = name;
   this.modelController = modelController;
   this.lastRefresh = null;
}

Node.prototype.refresh = function() {
   
}
