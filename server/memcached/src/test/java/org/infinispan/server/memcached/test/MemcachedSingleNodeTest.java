package org.infinispan.server.memcached.test;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.enableAuthentication;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.enableEncryption;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedServer;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.serverBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.DummyServerStateManager;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.server.memcached.text.TextConstants;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

/**
 * Base class for single node tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
public abstract class MemcachedSingleNodeTest extends SingleCacheManagerTest {
   protected MemcachedClient client;
   protected MemcachedServer server;
   protected static final int timeout = 60;
   protected boolean decoderReplay = true;
   protected final ControlledTimeService timeService = new ControlledTimeService("memcached", 1000L * TextConstants.MAX_EXPIRATION);

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = createTestCacheManager();
      GlobalComponentRegistry.of(cacheManager).registerComponent(new DummyServerStateManager(), ServerStateManager.class);
      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);
      MemcachedServerConfigurationBuilder builder = serverBuilder().protocol(getProtocol());
      if (withAuthentication()) {
         enableAuthentication(builder);
      }
      if (withEncryption()) {
         enableEncryption(builder);
      }
      server = MemcachedTestingUtil.createMemcachedServer(decoderReplay);
      startServer(server, builder);
      client = createMemcachedClient(server);
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
      return cacheManager;
   }

   protected void startServer(MemcachedServer server, MemcachedServerConfigurationBuilder builder) {
      server.start(builder.build(), cacheManager);
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManager(true);
   }

   protected abstract MemcachedProtocol getProtocol();

   protected boolean withAuthentication() {
      return false;
   }

   protected boolean withEncryption() {
      return false;
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

   public static void wait(OperationFuture<Boolean> f) throws ExecutionException, InterruptedException, TimeoutException {
      AssertJUnit.assertTrue(f.get(timeout, TimeUnit.SECONDS));
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
         assertEquals(expectedResp, resp);
      else
         assertTrue(resp.contains(expectedResp), "Expecting '" + expectedResp + ", got '" + resp + "' instead");
   }

}
