package org.infinispan.server.memcached.test;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.lang.reflect.Method;
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
}
