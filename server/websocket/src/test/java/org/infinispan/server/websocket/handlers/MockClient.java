package org.infinispan.server.websocket.handlers;

import org.codehaus.jackson.node.ObjectNode;
import org.infinispan.Cache;
import org.infinispan.manager.CacheContainer;
import org.infinispan.server.websocket.OpHandler;
import org.infinispan.server.websocket.json.JsonObject;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.websocket.MockChannel;
import org.infinispan.websocket.MockChannelHandlerContext;

/**
 * Mock client for testing.
 *
 * @author <a href="mailto:tom.fennelly@gmail.com">tom.fennelly@gmail.com</a>
 */
public class MockClient {

   private String cacheName;
   private CacheContainer cacheContainer;
   private Cache<Object, Object> cache;

   private OpHandler putHandler = new PutHandler();
   private OpHandler getHandler = new GetHandler();
   private OpHandler removeHandler = new RemoveHandler();
   private OpHandler notifyHandler = new NotifyHandler();
   private MockChannelHandlerContext ctx;

   public MockClient(String cacheName, MockChannel mockChannel) {
      this.cacheName = cacheName;
      this.ctx = new MockChannelHandlerContext(mockChannel);

      cacheContainer = TestCacheManagerFactory.createCacheManager();
      cache = cacheContainer.getCache(cacheName);
   }

   public void put(String key, String value) {
      callHandler(putHandler, toPut(key, value, "text/plain"));
   }

   public void put(String key, ObjectNode value) {
      callHandler(putHandler, toPut(key, value.getTextValue(), "application/json"));
   }

   public void get(String key) {
      callHandler(getHandler, toGet(key));
   }

   public void remove(String key) {
      callHandler(removeHandler, toRemove(key));
   }

   public void notify(String key) {
      callHandler(notifyHandler, toNotify(key));
   }

   public void unnotify(String key) {
      callHandler(notifyHandler, toUnnotify(key));
   }

   public Cache<Object, Object> getCache() {
      return cache;
   }

   private void callHandler(OpHandler handler, JsonObject jsonObj) {
      handler.handleOp(jsonObj, cache, ctx);
   }

   private JsonObject toPut(String key, String value, String mimeType) {
      JsonObject jsonObj = JsonObject.createNew();

      jsonObj.put(OpHandler.OP_CODE, "put");
      jsonObj.put(OpHandler.CACHE_NAME, cacheName);
      jsonObj.put(OpHandler.KEY, key);
      jsonObj.put(OpHandler.VALUE, value);
      jsonObj.put(OpHandler.MIME, mimeType);

      return jsonObj;
   }

   private JsonObject toGet(String key) {
      JsonObject jsonObj = JsonObject.createNew();

      jsonObj.put(OpHandler.OP_CODE, "get");
      jsonObj.put(OpHandler.CACHE_NAME, cacheName);
      jsonObj.put(OpHandler.KEY, key);

      return jsonObj;
   }

   private JsonObject toRemove(String key) {
      JsonObject jsonObj = JsonObject.createNew();

      jsonObj.put(OpHandler.OP_CODE, "remove");
      jsonObj.put(OpHandler.CACHE_NAME, cacheName);
      jsonObj.put(OpHandler.KEY, key);

      return jsonObj;
   }

   private JsonObject toNotify(String key) {
      JsonObject jsonObj = JsonObject.createNew();

      jsonObj.put(OpHandler.OP_CODE, "notify");
      jsonObj.put(OpHandler.CACHE_NAME, cacheName);
      jsonObj.put(OpHandler.KEY, key);

      return jsonObj;
   }

   private JsonObject toUnnotify(String key) {
      JsonObject jsonObj = JsonObject.createNew();

      jsonObj.put(OpHandler.OP_CODE, "unnotify");
      jsonObj.put(OpHandler.CACHE_NAME, cacheName);
      jsonObj.put(OpHandler.KEY, key);

      return jsonObj;
   }

   public void stop() {
      cache.clear();
      cacheContainer.stop();
   }

}
