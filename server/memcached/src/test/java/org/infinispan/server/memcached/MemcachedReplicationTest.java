package org.infinispan.server.memcached;

import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import net.spy.memcached.CASResponse;
import net.spy.memcached.CASValue;
import net.spy.memcached.internal.OperationFuture;

/**
 * Tests replicated Infinispan Memcached servers.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedReplicationTest")
public class MemcachedReplicationTest extends MemcachedMultiNodeTest {

   @Override
   protected EmbeddedCacheManager createCacheManager(int index) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // When replicating data, old append/prepend byte[] value in the cache
      // is a different instance (but maybe same contents) to the one shipped
      // by the replace command itself, hence for those tests to work, value
      // equivalence needs to be configured to compare byte array contents
      // and not instance reference pointers.
      builder.clustering().cacheMode(CacheMode.REPL_SYNC)
              .dataContainer().valueEquivalence(ByteArrayEquivalence.INSTANCE);
      return TestCacheManagerFactory.createClusteredCacheManager(
            GlobalConfigurationBuilder.defaultClusteredBuilder().defaultCacheName(cacheName),
            builder);
   }

   public void testReplicatedSet(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(0).get(k(m)), v(m));
   }

   public void testReplicatedGetMultipleKeys(Method m) throws InterruptedException, ExecutionException,
           TimeoutException {
      OperationFuture<Boolean> f1 = clients.get(0).set(k(m, "k1-"), 0, v(m, "v1-"));
      OperationFuture<Boolean> f2 = clients.get(0).set(k(m, "k2-"), 0, v(m, "v2-"));
      OperationFuture<Boolean> f3 = clients.get(0).set(k(m, "k3-"), 0, v(m, "v3-"));
      assertTrue(f1.get(timeout, TimeUnit.SECONDS));
      assertTrue(f2.get(timeout, TimeUnit.SECONDS));
      assertTrue(f3.get(timeout, TimeUnit.SECONDS));
      List<String> keys = Arrays.asList(k(m, "k1-"), k(m, "k2-"), k(m, "k3-"));
      Map<String, Object> ret = clients.get(1).getBulk(keys);
      assertEquals(ret.get(k(m, "k1-")), v(m, "v1-"));
      assertEquals(ret.get(k(m, "k2-")), v(m, "v2-"));
      assertEquals(ret.get(k(m, "k3-")), v(m, "v3-"));
   }

   public void testReplicatedAdd(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).add(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(1).get(k(m)), v(m));
   }

   public void testReplicatedReplace(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).add(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(1).get(k(m)), v(m));
      f = clients.get(1).replace(k(m), 0, v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(0).get(k(m)), v(m, "v1-"));
   }

   public void testReplicatedAppend(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).add(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(1).get(k(m)), v(m));
      f = clients.get(1).append(0, k(m), v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String expected = v(m) + v(m, "v1-");
      assertEquals(clients.get(0).get(k(m)), expected);
   }

   public void testReplicatedPrepend(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).add(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(1).get(k(m)), v(m));
      f = clients.get(1).prepend(0, k(m), v(m, "v1-"));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      String expected = v(m, "v1-") + v(m);
      assertEquals(clients.get(0).get(k(m)), expected);
  }

   public void testReplicatedGets(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      CASValue<Object> value = clients.get(1).gets(k(m));
      assertEquals(value.getValue(), v(m));
      assertTrue(value.getCas() != 0);
   }

   public void testReplicatedCasExists(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      CASValue<Object> value = clients.get(1).gets(k(m));
      assertEquals(value.getValue(), v(m));
      assertTrue(value.getCas() != 0);
      long old = value.getCas();
      CASResponse resp = clients.get(1).cas(k(m), value.getCas(), v(m, "v1-"));
      value = clients.get(0).gets(k(m));
      assertEquals(value.getValue(), v(m, "v1-"));
      assertTrue(value.getCas() != 0);
      assertTrue(value.getCas() != old);
      resp = clients.get(0).cas(k(m), old, v(m, "v2-"));
      assertEquals(resp, CASResponse.EXISTS);
      resp = clients.get(1).cas(k(m), value.getCas(), v(m, "v2-"));
      assertEquals(resp, CASResponse.OK);
   }

   public void testReplicatedDelete(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).set(k(m), 0, v(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      f = clients.get(1).delete(k(m));
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
   }

   public void testReplicatedIncrement(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(1).incr(k(m), 1), 2);
   }

   public void testReplicatedDecrement(Method m) throws InterruptedException, ExecutionException, TimeoutException {
      OperationFuture<Boolean> f = clients.get(0).set(k(m), 0, "1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      assertEquals(clients.get(1).decr(k(m), 1), 0);
   }

}
