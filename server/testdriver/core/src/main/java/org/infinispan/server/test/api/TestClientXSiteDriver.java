package org.infinispan.server.test.api;

import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.counter.api.CounterManager;

/**
 * Test client driver interface contains the methods we want to expose to be used from test methods
 *
 * @author Gustavo Lira
 * @since 12
 */
public interface TestClientXSiteDriver {

   /**
    * Get the HotRod instance for hotrod api operations
    *
    * @return {@link HotRodTestClientDriver} instance
    */
   HotRodTestClientDriver hotrod(String siteName);

   /**
    * Get the REST instance for hotrod api operations
    *
    * @return {@link RestTestClientDriver} instance}
    */
   RestTestClientDriver rest(String siteName);

   MemcachedTestClientDriver memcached(String siteName);

   /**
    * Provides the current method name
    *
    * @return String, the method name
    */
   String getMethodName();

   /**
    * Access to the {@link CounterManager} to perform counters operations on tests
    *
    * @return the {@link CounterManager} instance
    */
   CounterManager getCounterManager(String siteName);

   /**
    * Access to the {@link MultimapCacheManager} to perform multimap operations on tests.
    *
    * @return the {@link MultimapCacheManager} instance.
    */
   <K, V> MultimapCacheManager<K, V> getMultimapCacheManager(String siteName);
}
