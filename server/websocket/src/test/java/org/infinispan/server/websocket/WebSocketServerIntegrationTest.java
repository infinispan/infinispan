package org.infinispan.server.websocket;

import static org.infinispan.server.websocket.OpHandler.CACHE_NAME;
import static org.infinispan.server.websocket.OpHandler.ERROR;
import static org.infinispan.server.websocket.OpHandler.KEY;
import static org.infinispan.server.websocket.OpHandler.MIME;
import static org.infinispan.server.websocket.OpHandler.OP_CODE;
import static org.infinispan.server.websocket.OpHandler.VALUE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.WebSocket;
import org.eclipse.jetty.websocket.WebSocketClient;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.websocket.configuration.WebSocketServerConfigurationBuilder;
import org.infinispan.server.websocket.json.JsonObject;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import net.jcip.annotations.NotThreadSafe;

/**
 * @author gustavonalle
 */
@Test(testName = "websocket.WebSocketServerIntegrationTest", groups = "functional")
public class WebSocketServerIntegrationTest extends SingleCacheManagerTest {

   private WebSocketServer wsServer;
   private WebSocketTestClient wsClient;
   String cacheName = "testCache";

   @BeforeMethod
   public void setUp() throws Exception {
      Cache<String, String> cache = cacheManager.getCache(cacheName);
      cache.put("key", "value");

      wsServer = new WebSocketServer();
      wsServer.start(new WebSocketServerConfigurationBuilder().host("localhost").port(getFreePort()).build(), cacheManager);
      wsClient = new WebSocketTestClient(cacheName);
      wsClient.connect(new URI("ws://localhost:" + wsServer.getPort()));
   }

   @AfterMethod
   public void tearDown() throws Exception {
      wsServer.stop();
      wsClient.destroy();
   }

   @Test
   public void testGet() throws Exception {
      assertEquals(wsClient.get("key"), "value");
   }

   @Test
   public void testPut() throws Exception {
      String key = "key2";
      String value = "value2";
      wsClient.put(key, value);

      assertEquals(wsClient.get(key), value);
   }

   @Test
   public void testGarbage() throws Exception {
      JsonObject jsonObject = wsClient.sendAndWait("garbage");
      assertTrue(jsonObject.containsKey(ERROR));
   }

   @Test(expectedExceptions = Exception.class, expectedExceptionsMessageRegExp = "Cache temporarily unavailable")
   public void testDisableCache() throws Exception {
      assertEquals(wsClient.get("key"), "value");

      wsServer.ignoreCache(cacheName);

      assertEquals(wsClient.get("key"), "value");

   }

   @NotThreadSafe
   private static class WebSocketTestClient {

      private final String cacheName;
      private final WebSocketClient wsClient;
      private final WebSocketClientFactory webSocketClientFactory;
      private CountDownLatch messageLatch = new CountDownLatch(1);

      private static final int TIMEOUT = 5;
      private static final TimeUnit TIMEOUT_UNIT = TimeUnit.SECONDS;

      private WebSocket.Connection connection;
      private String lastMessage;

      public WebSocketTestClient(String cacheName) throws Exception {
         this.cacheName = cacheName;
         webSocketClientFactory = new WebSocketClientFactory();
         webSocketClientFactory.start();
         wsClient = webSocketClientFactory.newWebSocketClient();
      }

      public void connect(URI serverURI) throws Exception {
         connection = wsClient.open(serverURI, new ClientWebSocket()).get(2, TimeUnit.SECONDS);
      }

      public void destroy() throws Exception {
         webSocketClientFactory.stop();
         connection.close();
      }

      public void put(String key, String value) throws Exception {
         JsonObject jsonObject = JsonObject.createNew();
         jsonObject.put(OP_CODE, "put");
         jsonObject.put(CACHE_NAME, cacheName);
         jsonObject.put(KEY, key);
         jsonObject.put(VALUE, value);
         jsonObject.put(MIME, "text/plain");
         connection.sendMessage(jsonObject.toString());
      }

      public JsonObject sendAndWait(String message) throws Exception {
         messageLatch = new CountDownLatch(1);
         connection.sendMessage(message);
         messageLatch.await(TIMEOUT, TIMEOUT_UNIT);
         return JsonObject.fromString(lastMessage);
      }

      private class ClientWebSocket implements WebSocket.OnTextMessage {

         @Override
         public void onMessage(String data) {
            lastMessage = data;
            messageLatch.countDown();
         }

         @Override
         public void onOpen(Connection connection) {
         }

         @Override
         public void onClose(int closeCode, String message) {
         }
      }

      public String get(String key) throws Exception {
         JsonObject jsonObject = JsonObject.createNew();
         jsonObject.put(OP_CODE, "get");
         jsonObject.put(CACHE_NAME, cacheName);
         jsonObject.put(KEY, key);
         JsonObject response = sendAndWait(jsonObject.toString());
         if (response.containsKey(ERROR)) {
            throw new Exception(response.get(ERROR).toString());
         }
         return response.get("value").toString();
      }

   }

   private int getFreePort() throws IOException {
      try (ServerSocket socket = new ServerSocket(0)) {
         return socket.getLocalPort();
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(false);
   }
}
