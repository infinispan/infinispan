package org.infinispan.server.memcached.test;

import static org.infinispan.server.memcached.test.MemcachedTestingUtil.createMemcachedClient;
import static org.infinispan.server.memcached.test.MemcachedTestingUtil.killMemcachedClient;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.configuration.MemcachedProtocol;
import org.infinispan.server.memcached.configuration.MemcachedServerConfigurationBuilder;
import org.infinispan.testing.Exceptions;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import net.spy.memcached.internal.OperationFuture;

/**
 * Tests Memcached request that is larger than a configured limit on the Infinispan Server.
 */
@Test(groups = "functional", testName = "server.memcached.test.MemcachedRequestLimitTest")
public class MemcachedRequestLimitTest extends MemcachedSingleNodeTest {
   private MemcachedProtocol protocol;

   public MemcachedRequestLimitTest protocol(MemcachedProtocol protocol) {
      this.protocol = protocol;
      return this;
   }

   @Override
   public MemcachedProtocol getProtocol() {
      return protocol;
   }

   @Factory
   public Object[] factory() {
      return new Object[] {
            new MemcachedRequestLimitTest().protocol(MemcachedProtocol.BINARY),
            new MemcachedRequestLimitTest().protocol(MemcachedProtocol.TEXT),
      };
   }

   @Override
   protected String parameters() {
      return "[" + protocol + "]";
   }

   private static final int MAX_CONTENT_LENGTH = 128;

   public MemcachedRequestLimitTest() {
      // The test handlers install a 1 byte frame decoder, which means a request is never handed to the decoder in the
      // same read as another one. The pipelining test below relies on real reads to spot a request being charged for
      // bytes it already accounted for in an earlier read
      decoderReplay = false;
   }

   @Override
   protected void startServer(MemcachedServer server, MemcachedServerConfigurationBuilder builder) {
      super.startServer(server, builder.maxContentLength(Integer.toString(MAX_CONTENT_LENGTH)));
   }

   public void testKeyTooLong(Method m) {
      OperationFuture<Boolean> f = client.set(k(m, "k".repeat(MAX_CONTENT_LENGTH)), 0, v(m));
      Exceptions.expectException(ExecutionException.class, CancellationException.class, () -> f.get(10, TimeUnit.SECONDS));
   }

   public void testValueTooLong(Method m) {
      OperationFuture<Boolean> f = client.set(k(m), 0, v(m, "v".repeat(MAX_CONTENT_LENGTH)));
      Exceptions.expectException(ExecutionException.class, CancellationException.class, () -> f.get(10, TimeUnit.SECONDS));
   }

   /**
    * A request under the limit must still be accepted when the read it arrives in ends in the middle of it. The bytes
    * such a request consumed before the split used to be counted twice, so enough pipelined requests to span several
    * reads would see one rejected once a split landed far enough into a request.
    */
   public void testManyPipelinedRequestsNearLimit() throws Exception {
      // Enough requests, each just under the limit, that they cannot all be delivered in a single read
      int opCount = 512;
      List<OperationFuture<Boolean>> futures = new ArrayList<>(opCount);
      List<String> keys = new ArrayList<>(opCount);
      List<String> values = new ArrayList<>(opCount);
      for (int i = 0; i < opCount; ++i) {
         String key = "k" + i;
         // Varying the size keeps the requests from lining up with the read boundaries the same way every time, a
         // read has to end in the middle of a request for the double counting to show
         int valueLength = 84 - key.length() - i % 11;
         keys.add(key);
         values.add(Character.toString('a' + i % 26).repeat(valueLength));
         futures.add(client.set(key, 0, values.get(i)));
      }

      for (int i = 0; i < opCount; ++i) {
         assertEquals(Boolean.TRUE, futures.get(i).get(30, TimeUnit.SECONDS));
      }
      for (int i = 0; i < opCount; ++i) {
         assertEquals(values.get(i), client.get(keys.get(i)));
      }
   }

   public void testExcessDataDoesNotCorruptSubsequentConnection(Method m) throws Exception {
      String oversizedValue = "v".repeat(MAX_CONTENT_LENGTH * 4);
      OperationFuture<Boolean> f = client.set(k(m), 0, oversizedValue);
      Exceptions.expectException(ExecutionException.class, CancellationException.class, () -> f.get(10, TimeUnit.SECONDS));

      killMemcachedClient(client);
      client = createMemcachedClient(server);

      String key = k(m, "after");
      String value = "smallValue";
      wait(client.set(key, 0, value));
      assertEquals(value, client.get(key));
   }
}
