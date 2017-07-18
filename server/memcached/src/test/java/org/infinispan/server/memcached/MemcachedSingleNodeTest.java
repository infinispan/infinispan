package org.infinispan.server.memcached;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.startMemcachedTextServer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;

import net.spy.memcached.MemcachedClient;

/**
 * Base class for single node tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class MemcachedSingleNodeTest extends SingleCacheManagerTest {
   protected MemcachedClient client;
   protected MemcachedServer server;
   protected static final int timeout = 60;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = createTestCacheManager();
      server = startMemcachedTextServer(cacheManager);
      client = createMemcachedClient(60000, server.getPort());
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
      return cacheManager;
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManager(false);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      log.debug("Test finished, close memcached server");
      shutdownClient();
      killMemcachedServer(server);
   }

   protected void shutdownClient() {
      client.shutdown();
   }

   protected String send(String req) throws IOException {
      return sendMulti(req, 1, true).get(0);
   }

   protected List<String> sendNoWait(String req) throws IOException {
      return sendMulti(req, 1, false);
   }

   protected List<String> sendMulti(String req, int expectedResponses, boolean wait) throws IOException {
      try (Socket socket = new Socket(server.getHost(), server.getPort())) {
         OutputStream outputStream = socket.getOutputStream();
         outputStream.write(req.getBytes());
         outputStream.flush();
         if (wait) {
            Stream.Builder<String> builder = Stream.builder();
            for (int i = 0; i < expectedResponses; ++i) {
               builder.accept(readLine(socket.getInputStream(), new StringBuilder()));
            }
            return builder.build().collect(Collectors.toList());
         } else {
            return Collections.emptyList();
         }
      }
   }

   protected String readLine(InputStream is, StringBuilder sb) throws IOException {
      int next = is.read();
      if (next == 13) { // CR
         next = is.read();
         if (next == 10) { // LF
            return sb.toString().trim();
         } else {
            sb.append((char) next);
            return readLine(is, sb);
         }
      } else if (next == 10) { //LF
         return sb.toString().trim();
      } else {
         sb.append((char) next);
         return readLine(is, sb);
      }
   }

   protected void assertClientError(String resp) {
      assertExpectedResponse(resp, "CLIENT_ERROR", false);
   }

   protected void assertError(String resp) {
      assertExpectedResponse(resp, "ERROR", true);
   }

   protected void assertStored(String resp) {
      assertExpectedResponse(resp, "STORED", true);
   }

   protected void assertExpectedResponse(String resp, String expectedResp, boolean strictComparison) {
      if (strictComparison)
         assertEquals(resp, expectedResp, "Instead response is: " + resp);
      else
         assertTrue(resp.contains(expectedResp), "Instead response is: " + resp);
   }

}
