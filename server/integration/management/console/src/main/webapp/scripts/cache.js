/**
 * Represents a Cache
 */
function Cache(cluster, name, type) {
   this.cluster = cluster;
   this.name = name;
   this.type = type;
   this.modelController = cluster.getModelController();
   this.lastRefresh = null;
   this.data = null;
}

Cache.prototype.getModelController = function() {
   return this.modelController;
}

Cache.prototype.getResourcePath = function() {
   return this.cluster.getResourcePath().concat(this.type, this.name);
}

Cache.prototype.refresh = function(callback) {
   this.modelController.readResource(this.getResourcePath(), false, true, (function(response) {
      this.data = response;
      this.lastRefresh = new Date();
      callback(this);
   }).bind(this));
}
