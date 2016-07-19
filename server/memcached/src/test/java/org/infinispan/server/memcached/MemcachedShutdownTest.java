package org.infinispan.server.memcached;

import net.spy.memcached.internal.OperationFuture;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests that Infinispan Memcached server can shutdown even if client does not close connection.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedShutdownTest")
public class MemcachedShutdownTest extends MemcachedSingleNodeTest {
   @Override
   protected void shutdownClient() { }

   public void testAny(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = client.set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(client.get(k(m)), v(m));
   }

}