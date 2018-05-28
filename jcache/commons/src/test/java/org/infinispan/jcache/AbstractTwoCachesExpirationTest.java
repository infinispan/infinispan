package org.infinispan.jcache;

import static org.infinispan.jcache.util.JCacheTestingUtil.sleep;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import javax.cache.Cache;
import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryListenerException;

import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.util.ControlledTimeService;
import org.testng.annotations.Test;

/**
 * Base class for clustered JCache expiration tests. Implementations must provide cache references.
 *
 * @author Matej Cimbora
 */
public abstract class AbstractTwoCachesExpirationTest extends MultipleCacheManagersTest {

   protected final ControlledTimeService controlledTimeService = new ControlledTimeService();
   protected static final int EXPIRATION_TIMEOUT = 1000;

   @Test(groups = "functional")
   public void testExpiration(Method m) {
      Cache<String, String> cache1 = getCache1(m);
      Cache<String, String> cache2 = getCache2(m);

      TestExpiredListener listener = new TestExpiredListener();
      MutableCacheEntryListenerConfiguration conf1 = new MutableCacheEntryListenerConfiguration(FactoryBuilder.factoryOf(listener), null, false, false);
      cache1.registerCacheEntryListener(conf1);
      cache2.put(k(m), v(m));
      controlledTimeService.advance(EXPIRATION_TIMEOUT + 1000);
      eventually(() -> listener.invocationCount.get() >0);
      assertNull(cache1.get(k(m)));

      listener.invocationCount.set(0);
      cache1.deregisterCacheEntryListener(conf1);
      cache2.put(k(m, 2), v(m, 2));
      controlledTimeService.advance(EXPIRATION_TIMEOUT + 1000);
      assertNull(cache1.get(k(m, 2)));
      sleep(EXPIRATION_TIMEOUT);
      assertEquals(listener.invocationCount.get(), 0);
   }

   private static class TestExpiredListener implements CacheEntryExpiredListener, Serializable {

      private final AtomicInteger invocationCount = new AtomicInteger(0);

      @Override
      public void onExpired(Iterable iterable) throws CacheEntryListenerException {
         iterable.forEach(e -> invocationCount.incrementAndGet());
      }
   }

   public abstract Cache getCache1(Method m);
   public abstract Cache getCache2(Method m);
}
